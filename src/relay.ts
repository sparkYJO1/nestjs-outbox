import type { Pool } from "pg";
import type { Producer } from "kafkajs";

export interface RelayOptions {
  pool: Pool;
  producer: Producer;
  /** Rows per pass. Bounded so one pass cannot hold a connection indefinitely. */
  batchSize?: number;
  /** Idle wait between passes. Ignored while a pass returns a full batch. */
  intervalMs?: number;
  /** Rows exceeding this are left in place and reported, not retried forever. */
  maxAttempts?: number;
  onError?: (err: Error, row: { id: string; topic: string }) => void;
}

interface OutboxRow {
  id: string;
  topic: string;
  key: string;
  value: unknown;
  headers: Record<string, string>;
  attempts: number;
}

/**
 * Polls the outbox table and publishes. See ADR-0001 for why this is polling
 * and not logical replication.
 */
export class OutboxRelay {
  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private stopped = false;

  constructor(private readonly opts: RelayOptions) {}

  start(): void {
    this.stopped = false;
    void this.loop();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    // Let an in-flight pass finish rather than tearing down mid-publish.
    while (this.running) await new Promise((r) => setTimeout(r, 20));
  }

  private async loop(): Promise<void> {
    if (this.stopped) return;
    let published = 0;
    try {
      published = await this.pass();
    } catch (err) {
      this.opts.onError?.(err as Error, { id: "-", topic: "-" });
    }
    // A full batch means there is more waiting; go straight round again rather
    // than sleeping through a backlog.
    const wait =
      published >= (this.opts.batchSize ?? 100)
        ? 0
        : (this.opts.intervalMs ?? 500);
    this.timer = setTimeout(() => void this.loop(), wait);
  }

  /** One pass. Exposed so tests can drive it deterministically. */
  async pass(): Promise<number> {
    if (this.running) return 0;
    this.running = true;
    const client = await this.opts.pool.connect();
    try {
      await client.query("BEGIN");

      // FOR UPDATE SKIP LOCKED is what makes more than one relay safe: each
      // instance takes rows nobody else holds instead of blocking on them.
      // ORDER BY id keeps per-key order intact within a single pass.
      const { rows } = await client.query<OutboxRow>(
        `SELECT id, topic, key, value, headers, attempts
           FROM outbox
          WHERE published_at IS NULL
            AND attempts < $2
          ORDER BY id
          LIMIT $1
          FOR UPDATE SKIP LOCKED`,
        [this.opts.batchSize ?? 100, this.opts.maxAttempts ?? 10],
      );

      if (rows.length === 0) {
        await client.query("COMMIT");
        return 0;
      }

      const sent: string[] = [];
      for (const row of rows) {
        try {
          await this.opts.producer.send({
            topic: row.topic,
            messages: [
              {
                key: row.key,
                value: JSON.stringify(row.value),
                headers: { ...row.headers, "outbox-id": row.id },
              },
            ],
          });
          sent.push(row.id);
        } catch (err) {
          // Publishing failed. Record it and leave the row unpublished; the
          // next pass retries. Do not abandon the batch — one bad topic must
          // not hold up everything behind it.
          await client.query(
            `UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1`,
            [row.id, (err as Error).message.slice(0, 500)],
          );
          this.opts.onError?.(err as Error, { id: row.id, topic: row.topic });
        }
      }

      if (sent.length > 0) {
        await client.query(
          `UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])`,
          [sent],
        );
      }
      await client.query("COMMIT");
      return sent.length;
    } catch (err) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw err;
    } finally {
      client.release();
      this.running = false;
    }
  }
}
