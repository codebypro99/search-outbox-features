# Search Outbox Feature Samples

NestJS and TypeScript feature excerpts showing asynchronous Elasticsearch indexing from a PostgreSQL outbox, with notification-driven wakeups, scheduled retries, and separate paths for full document indexing and stock updates.

## Purpose

These samples demonstrate how I organize background processing around persisted work items. The worker coordinates indexing and records processing outcomes, while a separate listener reduces the delay between an outbox insertion and processing.

## Search Indexing Worker

**Source:** [search-worker.service.ts](./features/search-worker.service.ts)

Consumes due rows from `search_index_outbox` and translates them into Elasticsearch bulk operations.

### Implementation Highlights

- Waits for index initialization to succeed before draining the outbox.
- Selects up to 200 due rows with `FOR UPDATE SKIP LOCKED` inside a database transaction.
- Expands category-tree and collection events into affected product IDs through a loader service.
- Deduplicates product IDs and removes redundant stock updates when a full upsert is already scheduled.
- Builds full product documents and optionally enriches them with name embeddings.
- Uses partial updates for stock fields, avoiding document rebuilding and embedding calls on that path.
- When a configured embedder returns no vectors, indexes documents without vectors and schedules affected upsert rows for retry.
- Processes bulk response errors for product rows and promotes stock updates targeting missing documents to new full-upsert work items.
- Records successful processing or schedules retries with exponential backoff capped at five minutes.
- Marks rows as failed after 20 attempts and emits a structured `search_outbox_parked` log event.
- Polls every five seconds and supports immediate wakeups, including a remembered wakeup received during processing.
- Limits a drain cycle to ten batches and clears its scheduled timer on shutdown.

**What this demonstrates:** batch processing, differentiated indexing paths, persisted retry state, and operational visibility for failed background work.

## PostgreSQL Notification Listener

**Source:** [outbox-listener.service.ts](./features/outbox-listener.service.ts)

Uses a dedicated PostgreSQL connection to listen on the `search_outbox` channel and wake the worker when a notification arrives.

### Implementation Highlights

- Reads connection settings through NestJS configuration.
- Uses a dedicated `pg` client for the session-bound `LISTEN` subscription.
- Wakes the worker after connecting to process work that may have accumulated during disconnection.
- Schedules reconnection after connection errors or termination, with backoff capped at 30 seconds.
- Cancels its reconnect timer and closes its connection on application shutdown.
- Complements the worker's polling loop, which continues to schedule processing when notifications are unavailable.

**What this demonstrates:** separating latency optimization from the persisted work queue and managing a long-lived database subscription through application lifecycle hooks.

## Processing Flow

1. An application writes an outbox row; the database notification producer is outside these excerpts.
2. A notification wakes the worker, or its polling timer triggers processing.
3. The worker selects due rows and prepares document, stock, and deletion operations.
4. Elasticsearch receives the bulk request.
5. The worker records completion, schedules retries, or marks exhausted rows as failed.

## Technologies

NestJS, TypeScript, PostgreSQL, TypeORM transactions, the `pg` client, Elasticsearch bulk operations, and an external embedding-client abstraction.

## Excerpt Context

These are feature excerpts with descriptions. The outbox schema and insertion logic, notification trigger, index definitions, product loader, embedding client, and NestJS module wiring belong to the surrounding application.

Elasticsearch writes and PostgreSQL completion markers are separate operations. A rollback after an external write can leave the same outbox work eligible for processing again; this excerpt should not be interpreted as an atomic transaction across both systems.

Per-item Elasticsearch error attribution is implemented for product rows. Errors on products expanded from category-tree or collection events are not attributed back to those source rows in this excerpt. Reconciliation mentioned in source comments is outside the included files.

Descriptions are based on source inspection. The excerpts have not been compiled or executed in isolation.

## Portfolio Message

I can structure background search-indexing features with persisted work queues, bulk processing, retry policies, notification-driven wakeups, and explicit failure reporting.
