import type { Redis } from "ioredis";

export interface IdempotencyOptions {
  redis: Redis;
  /** How long a processed key is remembered. See ADR-0002 — this is a real trade. */
  ttlSeconds?: number;
  keyPrefix?: string;
}

export type Outcome = "processed" | "duplicate";

/**
 * At-least-once delivery means the handler will see the same message twice.
 * This makes the second one cheap instead of harmful.
 *
 * It does **not** make the system exactly-once, and the README says so. The
 * window is the TTL: a duplicate arriving after it expires is processed again.
 * Claiming otherwise is the fastest way to fail an interview on this topic.
 */
export class IdempotencyGuard {
  private readonly prefix: string;
  private readonly ttl: number;

  constructor(private readonly opts: IdempotencyOptions) {
    this.prefix = opts.keyPrefix ?? "idem";
    this.ttl = opts.ttlSeconds ?? 60 * 60 * 24 * 7;
  }

  /**
   * Reserve-then-run, not run-then-mark.
   *
   * SET NX is the reservation. Doing it first means two concurrent deliveries
   * of the same message cannot both enter the handler. Marking afterwards
   * would leave exactly that race open, which is the common way this is
   * written and the reason it appears to work until it is under load.
   */
  async run(messageId: string, handler: () => Promise<void>): Promise<Outcome> {
    const key = `${this.prefix}:${messageId}`;
    const reserved = await this.opts.redis.set(
      key,
      "in-flight",
      "EX",
      this.ttl,
      "NX",
    );
    if (reserved === null) return "duplicate";

    try {
      await handler();
      await this.opts.redis.set(key, "done", "EX", this.ttl);
      return "processed";
    } catch (err) {
      // The reservation is released so a retry is not swallowed as a duplicate.
      // The cost of releasing is a possible double-run if the process dies
      // between handler success and this line; the cost of not releasing is a
      // message silently never processed. Losing work is worse than repeating
      // it, so the guard fails open.
      await this.opts.redis.del(key).catch(() => undefined);
      throw err;
    }
  }

  /** Whether a message id is currently known. Exposed for tests and metrics. */
  async seen(messageId: string): Promise<boolean> {
    return (await this.opts.redis.exists(`${this.prefix}:${messageId}`)) === 1;
  }
}
