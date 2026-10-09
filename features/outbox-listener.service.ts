import { Injectable, Logger, OnModuleInit, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Client } from 'pg';
import { SearchWorkerService } from './search-worker.service';

const RECONNECT_BASE_MS = 1_000;
const RECONNECT_MAX_MS = 30_000;

/**
 * LISTENs on the `search_outbox` channel (NOTIFY fired by a DB trigger on outbox
 * insert — see migration AddSearchOutboxNotify) and wakes the drain loop, cutting
 * sync latency from the 5s poll to near-instant.
 *
 * Uses a dedicated `pg` connection: LISTEN requires a session pinned to one backend,
 * which a pooled TypeORM connection can't guarantee. If the connection drops, it
 * reconnects with capped backoff; while it's down, the worker's 5s poll still drains,
 * so this service failing only costs latency, never correctness.
 */
@Injectable()
export class OutboxListenerService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(OutboxListenerService.name);
  private client: Client | null = null;
  private stopped = false;
  private reconnectAttempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;

  constructor(
    private readonly config: ConfigService,
    private readonly worker: SearchWorkerService,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.connect();
  }

  async onApplicationShutdown(): Promise<void> {
    this.stopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    await this.client?.end().catch(() => undefined);
    this.client = null;
  }

  private async connect(): Promise<void> {
    if (this.stopped) return;
    const client = new Client({
      host: this.config.get('DB_HOST', 'localhost'),
      port: parseInt(this.config.get('DB_PORT', '5432'), 10),
      user: this.config.get('DB_USERNAME', 'postgres'),
      password: this.config.get('DB_PASSWORD', 'postgres'),
      database: this.config.get('DB_DATABASE', 'warehouse'),
    });

    client.on('notification', () => this.worker.wake());
    client.on('error', (err) => {
      this.logger.warn(`LISTEN connection error: ${err.message}`);
      this.scheduleReconnect(client);
    });
    client.on('end', () => this.scheduleReconnect(client));

    try {
      await client.connect();
      await client.query('LISTEN search_outbox');
      this.client = client;
      this.reconnectAttempts = 0;
      this.logger.log('Listening on search_outbox channel');
      // Rows may have been inserted while we were disconnected — drain once now.
      this.worker.wake();
    } catch (err) {
      this.logger.warn(`LISTEN connect failed: ${(err as Error).message}`);
      await client.end().catch(() => undefined);
      this.scheduleReconnect(client);
    }
  }

  private scheduleReconnect(from: Client): void {
    if (this.stopped || this.reconnectTimer) return;
    if (this.client === from) this.client = null;
    const delay = Math.min(RECONNECT_BASE_MS * 2 ** Math.min(this.reconnectAttempts++, 5), RECONNECT_MAX_MS);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connect();
    }, delay);
  }
}
