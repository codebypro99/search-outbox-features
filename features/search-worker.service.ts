import { Inject, Injectable, Logger, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectDataSource } from '@nestjs/typeorm';
import { Client } from '@elastic/elasticsearch';
import { DataSource } from 'typeorm';
import { ES_CLIENT } from '../../core-api/src/search/elasticsearch.provider';
import { EmbedderClient } from '../../core-api/src/search/embedder.client';
import { ProductLoaderService, ProductDoc } from './product-loader.service';
import { ensureIndex, PRODUCTS_CURRENT_INDEX } from './search-index';

interface OutboxRow {
  id: string;
  entity_type: 'product' | 'category-tree' | 'collection';
  entity_id: string;
  op: 'upsert' | 'delete' | 'stock';
  attempts: number;
}

const POLL_INTERVAL_MS = 5_000;
const BATCH_SIZE = 200;
const MAX_ATTEMPTS = 20;
const BACKOFF_BASE_MS = 5_000;
const BACKOFF_MAX_MS = 5 * 60_000;

/**
 * The search-worker drain loop.
 *
 * Tick:
 *   1. Pull up to BATCH_SIZE outbox rows that are due (`next_attempt_at <= now()`),
 *      locking them with `FOR UPDATE SKIP LOCKED` so multiple worker replicas can share
 *      the table safely (current deploy is 1 replica, but the design admits scale-out).
 *   2. Expand 'category-tree' / 'collection' events into the corresponding product ids.
 *   3. Load the resulting product docs from Postgres. 'stock' ops take a fast path:
 *      partial `_update` of the stock fields only — no doc rebuild, NO embedder call.
 *   4. Get embeddings from the GPU host (skipped if EMBEDDER_URL is unset — docs go in
 *      without `name_vector`, BM25 still works). If EMBEDDER_URL is set but the call
 *      fails, docs are still indexed vector-less so lexical search stays fresh, but the
 *      rows are retried with backoff until the vectors land.
 *   5. Bulk-index to Elasticsearch.
 *   6. Mark rows processed. On failure, bump `attempts` + push out `next_attempt_at`
 *      with exponential backoff. After MAX_ATTEMPTS, mark `failed_at` and emit the
 *      `search_outbox_parked` structured log (Loki alert target). Parked rows are
 *      auto-unparked once by the nightly reconciliation run.
 *
 * Latency: OutboxListenerService LISTENs on the `search_outbox` channel (NOTIFY fires
 * from a DB trigger on outbox insert) and calls `wake()`, so drains normally start
 * within milliseconds of a commit. The 5s poll remains as the retry scheduler and as
 * the fallback when the LISTEN connection is down.
 *
 * The lock + processed_at update happen in the same TX, so a worker crash mid-batch
 * leaves the rows for the next tick (no manual cleanup needed).
 */
@Injectable()
export class SearchWorkerService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(SearchWorkerService.name);
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private stopped = false;
  private indexReady = false;
  private pendingWake = false;

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject(ES_CLIENT) private readonly es: Client | null,
    private readonly embedder: EmbedderClient,
    private readonly loader: ProductLoaderService,
    private readonly config: ConfigService,
  ) {}

  async onModuleInit(): Promise<void> {
    if (!this.es) {
      this.logger.error('ES_URL is not set — search-worker has nothing to do; staying idle.');
      return;
    }
    await this.tryEnsureIndex();
    this.scheduleNext();
  }

  onApplicationShutdown(): void {
    this.stopped = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  /**
   * Called by OutboxListenerService on NOTIFY. If idle, drain immediately; if a drain
   * is running, remember the wake so the finally-block reschedules without the 5s wait
   * (rows committed mid-drain would otherwise sit until the next poll).
   */
  wake(): void {
    if (this.stopped || !this.es) return;
    if (this.running) {
      this.pendingWake = true;
      return;
    }
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.tick().catch((err) => this.logger.error(`wake tick crashed: ${(err as Error).message}`));
  }

  private scheduleNext(delayMs = POLL_INTERVAL_MS): void {
    if (this.stopped) return;
    this.timer = setTimeout(() => {
      this.tick().catch((err) => this.logger.error(`tick crashed: ${(err as Error).message}`));
    }, delayMs);
  }

  /**
   * Indexing MUST NOT start before ensureIndex has succeeded: a bulk write against a
   * missing index makes ES auto-create it with dynamic mappings (tenantIds as analyzed
   * text, no custom analyzer, no dense_vector) and every search silently returns zero
   * hits. This happened on staging when the worker booted faster than ES.
   */
  private async tryEnsureIndex(): Promise<boolean> {
    if (this.indexReady || !this.es) return this.indexReady;
    try {
      await ensureIndex(this.es);
      this.indexReady = true;
    } catch (err) {
      this.logger.error(
        `ensureIndex failed — retrying next tick, not draining until it succeeds: ${(err as Error).message}`,
      );
    }
    return this.indexReady;
  }

  private async tick(): Promise<void> {
    if (this.running || !this.es) {
      this.scheduleNext();
      return;
    }
    if (!(await this.tryEnsureIndex())) {
      this.scheduleNext();
      return;
    }
    this.running = true;
    try {
      let processed = 0;
      // Drain in a loop while there are due rows — keeps backlog small after an outage.
      // Cap the inner loop so a huge backfill doesn't starve other concerns.
      for (let i = 0; i < 10; i++) {
        const handled = await this.drainBatch();
        processed += handled;
        if (handled < BATCH_SIZE) break;
      }
      if (processed > 0) {
        this.logger.log(`Processed ${processed} outbox row(s)`);
      }
    } finally {
      this.running = false;
      const wakeRequested = this.pendingWake;
      this.pendingWake = false;
      // A NOTIFY arrived mid-drain: re-run almost immediately instead of waiting 5s.
      this.scheduleNext(wakeRequested ? 50 : POLL_INTERVAL_MS);
    }
  }

  private async drainBatch(): Promise<number> {
    return this.dataSource.transaction(async (em) => {
      const rows: OutboxRow[] = await em.query(
        `
        SELECT id, entity_type, entity_id, op, attempts
        FROM search_index_outbox
        WHERE processed_at IS NULL AND failed_at IS NULL AND next_attempt_at <= now()
        ORDER BY id
        LIMIT $1
        FOR UPDATE SKIP LOCKED
        `,
        [BATCH_SIZE],
      );
      if (rows.length === 0) return 0;

      // 1. Expand category-tree / collection rows into the affected product ids.
      const productUpserts = new Set<string>();
      const productDeletes = new Set<string>();
      const stockUpdates = new Set<string>();
      for (const row of rows) {
        if (row.entity_type === 'product') {
          if (row.op === 'delete') productDeletes.add(row.entity_id);
          else if (row.op === 'stock') stockUpdates.add(row.entity_id);
          else productUpserts.add(row.entity_id);
        } else if (row.entity_type === 'category-tree') {
          const ids = await this.loader.descendantProductIds(row.entity_id);
          ids.forEach((id) => productUpserts.add(id));
        } else if (row.entity_type === 'collection') {
          const ids = await this.loader.collectionProductIds(row.entity_id);
          ids.forEach((id) => productUpserts.add(id));
        }
      }
      // A full upsert already carries fresh stock fields — drop redundant stock ops.
      for (const id of [...stockUpdates]) {
        if (productUpserts.has(id)) stockUpdates.delete(id);
      }

      // 2. Build the ES bulk body. Process upserts and deletes separately so we don't have
      //    to mix tombstones into the embedder call.
      const bulkOps: any[] = [];
      // Embedder configured but the embed call failed. We still index the docs (keeps
      // lexical search fresh) but the affected upsert rows are retried with backoff so
      // the vectors eventually land — otherwise those products silently drop out of kNN
      // until their next edit. Upserts overwrite, so the retry is idempotent.
      let embedFailed = false;

      if (productUpserts.size > 0) {
        const docs = await this.loader.load([...productUpserts]);
        const names = docs.map((d) => d.doc.name);
        const vectors = this.embedder.enabled ? await this.embedder.embed(names) : null;
        embedFailed = this.embedder.enabled && docs.length > 0 && vectors === null;
        for (let i = 0; i < docs.length; i++) {
          const enriched: ProductDoc & { name_vector?: number[] } = { ...docs[i].doc };
          if (vectors?.[i]) {
            enriched.name_vector = vectors[i];
          }
          bulkOps.push({ index: { _index: PRODUCTS_CURRENT_INDEX, _id: docs[i].id } });
          bulkOps.push(enriched);
        }
      }
      // Stock fast path: partial update of the stock fields only. No doc rebuild, no
      // embedder — this path fires on every sale/receipt and must never touch the GPU.
      if (stockUpdates.size > 0) {
        const stockFields = await this.loader.stockFields([...stockUpdates]);
        for (const [id, fields] of stockFields) {
          bulkOps.push({ update: { _index: PRODUCTS_CURRENT_INDEX, _id: id } });
          bulkOps.push({ doc: fields });
        }
      }
      for (const id of productDeletes) {
        bulkOps.push({ delete: { _index: PRODUCTS_CURRENT_INDEX, _id: id } });
      }

      const succeededRowIds: string[] = [];
      const failedRows: Array<{ row: OutboxRow; error: string }> = [];
      // Stock updates hitting a doc that isn't indexed yet (out-of-order with the first
      // upsert): promote to a full upsert row and consider the stock row done.
      const promoteToUpsert = new Set<string>();

      // Rows whose effect is an upsert (needs a vector): product upserts and every
      // category-tree / collection row (those always expand to upserts). Product
      // deletes and stock ops never need vectors.
      const isUpsertRow = (row: OutboxRow) =>
        row.entity_type === 'category-tree' || row.entity_type === 'collection' || row.op === 'upsert';
      const markSucceeded = (row: OutboxRow) => {
        if (embedFailed && isUpsertRow(row)) {
          failedRows.push({ row, error: 'embedder unavailable — indexed without vector, retrying' });
        } else {
          succeededRowIds.push(row.id);
        }
      };

      if (bulkOps.length > 0) {
        try {
          const bulkRes = await this.es!.bulk({ operations: bulkOps, refresh: false });
          if (bulkRes.errors) {
            // ES returned per-item errors. Walk items, gather any docs that failed; mark
            // the corresponding outbox rows for retry rather than the whole batch.
            const failedIds = new Set<string>();
            const missingIds = new Set<string>();
            for (const item of bulkRes.items) {
              const op = item.index ?? item.delete ?? item.create ?? item.update;
              if (op && op.error) {
                if (item.update && op.error.type === 'document_missing_exception') {
                  missingIds.add(op._id as string);
                } else {
                  failedIds.add(op._id as string);
                  this.logger.warn(`ES rejected ${op._id}: ${JSON.stringify(op.error)}`);
                }
              }
            }
            for (const row of rows) {
              // category-tree / collection rows can't be partial-failed at row level here
              const affected = row.entity_type === 'product' ? failedIds.has(row.entity_id) : false;
              if (affected) {
                failedRows.push({ row, error: 'ES per-item failure' });
              } else if (row.entity_type === 'product' && row.op === 'stock' && missingIds.has(row.entity_id)) {
                promoteToUpsert.add(row.entity_id);
                succeededRowIds.push(row.id);
              } else {
                markSucceeded(row);
              }
            }
          } else {
            rows.forEach(markSucceeded);
          }
        } catch (err) {
          // Whole-batch failure (ES down, embedder down, network). Push everything back
          // with backoff — we'll retry the entire batch next tick.
          for (const row of rows) failedRows.push({ row, error: (err as Error).message });
        }
      } else {
        // Nothing to send (e.g. all rows pointed at products that no longer exist).
        succeededRowIds.push(...rows.map((r) => r.id));
      }

      if (promoteToUpsert.size > 0) {
        await em.query(
          `INSERT INTO search_index_outbox (entity_type, entity_id, op)
           SELECT 'product', unnest($1::text[]), 'upsert'`,
          [[...promoteToUpsert]],
        );
      }

      if (succeededRowIds.length > 0) {
        await em.query(`UPDATE search_index_outbox SET processed_at = now() WHERE id = ANY($1)`, [succeededRowIds]);
      }

      for (const { row, error } of failedRows) {
        const attempts = row.attempts + 1;
        if (attempts >= MAX_ATTEMPTS) {
          // Structured event for Loki alerting: parked rows mean ES and pg have drifted
          // until the nightly reconciliation unparks them (or a human intervenes).
          this.logger.error(
            JSON.stringify({
              event: 'search_outbox_parked',
              rowId: row.id,
              entityType: row.entity_type,
              entityId: row.entity_id,
              op: row.op,
              attempts,
              error: error.slice(0, 500),
            }),
          );
          await em.query(
            `UPDATE search_index_outbox SET attempts = $1, failed_at = now(), last_error = $2 WHERE id = $3`,
            [attempts, error.slice(0, 1000), row.id],
          );
        } else {
          const backoffMs = Math.min(BACKOFF_BASE_MS * 2 ** Math.min(attempts, 10), BACKOFF_MAX_MS);
          await em.query(
            `UPDATE search_index_outbox
             SET attempts = $1, next_attempt_at = now() + ($2 || ' milliseconds')::interval, last_error = $3
             WHERE id = $4`,
            [attempts, backoffMs, error.slice(0, 1000), row.id],
          );
        }
      }

      return rows.length;
    });
  }
}
