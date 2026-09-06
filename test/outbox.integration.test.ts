import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { Pool } from "pg";
import Redis from "ioredis";
import { Kafka, logLevel } from "kafkajs";
import { IdempotencyGuard, OUTBOX_SCHEMA, OutboxRelay, enqueue } from "../src";

// Real Postgres, real Redpanda, real Redis. `npm run infra:up` first.
// A mock cannot tell you whether the row and the business write share a
// transaction, which is the only thing this library promises.
const pool = new Pool({
  connectionString: `postgres://postgres:postgres@localhost:${process.env.PGPORT ?? 55432}/outbox_test`,
});
const redis = new Redis(`redis://localhost:${process.env.REDISPORT ?? 56379}`);
const kafka = new Kafka({
  clientId: "outbox-test",
  brokers: ["localhost:19092"],
  logLevel: logLevel.NOTHING,
});
const producer = kafka.producer();

const TOPIC = "outbox-test-topic";

beforeAll(async () => {
  await pool.query(OUTBOX_SCHEMA);
  await pool.query(
    `CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, total INT NOT NULL)`,
  );
  await producer.connect();
});

beforeEach(async () => {
  await pool.query("TRUNCATE outbox, orders");
  await redis.flushdb();
});

afterAll(async () => {
  await producer.disconnect();
  await pool.end();
  redis.disconnect();
});

describe("enqueue", () => {
  it("writes the row in the caller transaction, so a rollback takes it too", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`INSERT INTO orders (id, total) VALUES ('o1', 100)`);
      await enqueue(client, { topic: TOPIC, key: "o1", value: { id: "o1" } });
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }

    // This is the assertion the whole pattern exists for: no order, and no
    // event about an order that does not exist.
    expect(
      (await pool.query("SELECT count(*)::int n FROM orders")).rows[0].n,
    ).toBe(0);
    expect(
      (await pool.query("SELECT count(*)::int n FROM outbox")).rows[0].n,
    ).toBe(0);
  });

  it("commits the row with the business write", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`INSERT INTO orders (id, total) VALUES ('o2', 200)`);
      await enqueue(client, { topic: TOPIC, key: "o2", value: { id: "o2" } });
      await client.query("COMMIT");
    } finally {
      client.release();
    }
    expect(
      (await pool.query("SELECT count(*)::int n FROM orders")).rows[0].n,
    ).toBe(1);
    expect(
      (await pool.query("SELECT count(*)::int n FROM outbox")).rows[0].n,
    ).toBe(1);
  });

  it("refuses a client that is not in a transaction — the case types cannot catch", async () => {
    const client = await pool.connect();
    try {
      // A real PoolClient of the right type, with no BEGIN issued on it. This
      // is what actually compiles — a Pool would not. Only the runtime check
      // separates them.
      await expect(
        enqueue(client, { topic: TOPIC, key: "x", value: {} }),
      ).rejects.toThrow(/must be called inside a transaction/);
    } finally {
      client.release();
    }
  });
});

describe("OutboxRelay", () => {
  it("publishes unsent rows and marks them, exactly once per row", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      for (const id of ["r1", "r2", "r3"]) {
        await enqueue(client, { topic: TOPIC, key: id, value: { id } });
      }
      await client.query("COMMIT");
    } finally {
      client.release();
    }

    const relay = new OutboxRelay({ pool, producer, batchSize: 10 });
    expect(await relay.pass()).toBe(3);

    // A second pass has nothing to do — published rows are not re-sent.
    expect(await relay.pass()).toBe(0);

    const { rows } = await pool.query(
      "SELECT count(*)::int n FROM outbox WHERE published_at IS NOT NULL",
    );
    expect(rows[0].n).toBe(3);
  });

  it("leaves a row unpublished and counts the attempt when publishing fails", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await enqueue(client, { topic: TOPIC, key: "bad", value: { id: "bad" } });
      await client.query("COMMIT");
    } finally {
      client.release();
    }

    const failing = {
      send: async () => {
        throw new Error("broker unavailable");
      },
    } as unknown as typeof producer;

    const errors: string[] = [];
    const relay = new OutboxRelay({
      pool,
      producer: failing,
      onError: (e) => errors.push(e.message),
    });

    expect(await relay.pass()).toBe(0);
    const { rows } = await pool.query(
      "SELECT attempts, published_at, last_error FROM outbox",
    );
    expect(rows[0].attempts).toBe(1);
    expect(rows[0].published_at).toBeNull();
    expect(rows[0].last_error).toMatch(/broker unavailable/);
    expect(errors).toHaveLength(1);

    // And it recovers on its own once the broker is back.
    expect(await new OutboxRelay({ pool, producer }).pass()).toBe(1);
  });

  it("stops retrying a row past maxAttempts instead of looping forever", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await enqueue(client, { topic: TOPIC, key: "stuck", value: {} });
      await client.query("COMMIT");
    } finally {
      client.release();
    }
    await pool.query("UPDATE outbox SET attempts = 10");

    const relay = new OutboxRelay({ pool, producer, maxAttempts: 10 });
    expect(await relay.pass()).toBe(0);
    // Still there, still unpublished, and no longer being picked up.
    const { rows } = await pool.query(
      "SELECT count(*)::int n FROM outbox WHERE published_at IS NULL",
    );
    expect(rows[0].n).toBe(1);
  });
});

describe("IdempotencyGuard", () => {
  it("runs once and reports the second delivery as a duplicate", async () => {
    const guard = new IdempotencyGuard({ redis });
    let runs = 0;
    const handler = async () => {
      runs += 1;
    };

    expect(await guard.run("m1", handler)).toBe("processed");
    expect(await guard.run("m1", handler)).toBe("duplicate");
    expect(runs).toBe(1);
  });

  it("reserves before running, so concurrent deliveries cannot both enter", async () => {
    const guard = new IdempotencyGuard({ redis });
    let concurrent = 0;
    let maxConcurrent = 0;
    const slow = async () => {
      concurrent += 1;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise((r) => setTimeout(r, 50));
      concurrent -= 1;
    };

    const results = await Promise.all([
      guard.run("m2", slow),
      guard.run("m2", slow),
      guard.run("m2", slow),
    ]);
    expect(results.filter((r) => r === "processed")).toHaveLength(1);
    expect(results.filter((r) => r === "duplicate")).toHaveLength(2);
    expect(maxConcurrent).toBe(1);
  });

  it("releases the reservation when the handler throws, so a retry is not swallowed", async () => {
    const guard = new IdempotencyGuard({ redis });
    await expect(
      guard.run("m3", async () => {
        throw new Error("handler blew up");
      }),
    ).rejects.toThrow("handler blew up");

    expect(await guard.seen("m3")).toBe(false);
    expect(await guard.run("m3", async () => undefined)).toBe("processed");
  });

  it("processes a duplicate again once the TTL has expired — at-least-once, not exactly-once", async () => {
    const guard = new IdempotencyGuard({ redis, ttlSeconds: 1 });
    let runs = 0;
    const handler = async () => {
      runs += 1;
    };

    expect(await guard.run("m4", handler)).toBe("processed");
    await new Promise((r) => setTimeout(r, 1200));
    // This is the honest limit of the design, asserted rather than hidden.
    expect(await guard.run("m4", handler)).toBe("processed");
    expect(runs).toBe(2);
  });
});
