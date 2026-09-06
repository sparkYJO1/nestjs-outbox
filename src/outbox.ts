import type { PoolClient } from "pg";

export interface OutboxMessage {
  topic: string;
  key: string;
  value: unknown;
  headers?: Record<string, string>;
}

/**
 * The whole point of the pattern, in one function.
 *
 * `enqueue` takes the caller's transaction client. It does not open its own.
 * That is the difference between an outbox and a queue: the row lands in the
 * same transaction as the business write, so there is no window where the order
 * exists and the event does not, or the reverse.
 *
 * The type only gets you halfway. A `Pool` will not type-check here, but a
 * `PoolClient` taken from `pool.connect()` with no `BEGIN` issued on it will —
 * right type, no transaction, guarantee silently gone. Nothing in the type
 * system separates those two clients, which is why `assertInTransaction` runs.
 */
export async function enqueue(
  tx: PoolClient,
  message: OutboxMessage,
): Promise<void> {
  await assertInTransaction(tx);
  await tx.query(
    `INSERT INTO outbox (topic, key, value, headers) VALUES ($1, $2, $3, $4)`,
    [
      message.topic,
      message.key,
      JSON.stringify(message.value),
      JSON.stringify(message.headers ?? {}),
    ],
  );
}

/**
 * Postgres reports the current transaction state in `pg_stat_activity`, but the
 * cheap check is simpler: outside a transaction every statement auto-commits,
 * so a savepoint has nothing to attach to and Postgres refuses it with
 * `25P01 no_active_sql_transaction`. We use that. (It is class 25 —
 * invalid transaction state — not a syntax error.)
 */
async function assertInTransaction(tx: PoolClient): Promise<void> {
  try {
    await tx.query("SAVEPOINT outbox_tx_check");
    await tx.query("RELEASE SAVEPOINT outbox_tx_check");
  } catch {
    throw new Error(
      "enqueue() must be called inside a transaction. " +
        "A PoolClient with no BEGIN issued on it compiles fine and removes " +
        "the only guarantee this library provides.",
    );
  }
}

export const OUTBOX_SCHEMA = `
  CREATE TABLE IF NOT EXISTS outbox (
    id          BIGSERIAL PRIMARY KEY,
    topic       TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       JSONB NOT NULL,
    headers     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts    INT NOT NULL DEFAULT 0,
    last_error  TEXT
  );
  -- The relay only ever reads unpublished rows, so the index covers exactly
  -- that. A plain index on created_at would scan published rows forever.
  CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (id) WHERE published_at IS NULL;
`;
