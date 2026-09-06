# nestjs-outbox

Transactional outbox and idempotent consumers for NestJS, Postgres, and
Kafka-compatible brokers.

Small on purpose. It solves one problem — the gap between "the row is committed"
and "the event is published" — and refuses to solve anything else.

```ts
import { enqueue, OutboxRelay, IdempotencyGuard } from 'nestjs-outbox';

// Producer. The event lands in the same transaction as the write.
await client.query('BEGIN');
await client.query('INSERT INTO orders (id, total) VALUES ($1, $2)', [id, total]);
await enqueue(client, { topic: 'orders', key: id, value: { id, total } });
await client.query('COMMIT');

// Relay. Polls, publishes, marks. Safe to run more than one.
new OutboxRelay({ pool, producer }).start();

// Consumer. Makes the second delivery cheap.
await guard.run(messageId, async () => handle(message));
```

## The bug this exists to prevent

```ts
await db.insertOrder(order);      // committed
await kafka.send(orderCreated);   // throws
```

The order exists and nothing downstream will ever hear about it. Swap the two
lines and you get the opposite bug: an event about an order that does not
exist. There is no ordering of those two statements that is correct, because
they are two systems and one of them is going to fail eventually.

The outbox makes it one write. The event goes into a table in the same
transaction as the business row, and a relay publishes it afterwards.

`enqueue` takes a transaction client and **rejects a pool client at runtime**.
Passing a pool type-checks perfectly and silently removes the only guarantee
this library provides, so it fails loudly instead.

## What it promises, and what it does not

**Promises:** the event row and the business write commit or roll back
together. A published row is never published twice by the relay. Concurrent
deliveries of one message id cannot both enter the handler.

**Does not promise: exactly-once.** This is at-least-once with a deduplication
window. A duplicate arriving after the TTL expires **is processed again**, and
there is a test asserting that rather than a comment hoping you do not notice.
Handlers still need to be idempotent; this makes that cheap, not unnecessary.

## The two decisions worth arguing about

**[ADR-0001 — poll the table, do not tail the WAL](docs/decisions/0001-poll-the-table-do-not-tail-the-wal.md)**
CDC wins on paper. It also needs a replication slot that holds WAL forever if
nobody consumes it, `wal_level=logical`, and its own monitoring. On a
five-person team with no dedicated infrastructure engineer, its failure mode is
a disk filling at 3am. Polling's failure mode is a few hundred milliseconds of
latency. That trade only becomes obvious once you say who is on call.

**[ADR-0002 — at-least-once, and saying so](docs/decisions/0002-at-least-once-and-saying-so.md)**
Why the dedup window is a TTL, what each possible TTL is actually protecting
against, why the guard reserves before running rather than marking after, and
why it fails open.

## Tests

Integration tests run against **real Postgres, Redpanda and Redis**. A mock
cannot tell you whether the outbox row and the business write share a
transaction, which is the only thing being claimed.

```bash
npm run infra:up
npm run test:integration    # 10 tests
npm run infra:down
```

They assert the uncomfortable cases too: rollback takes the event with it, a
pool client is rejected, a failed publish leaves the row and increments
`attempts`, a row past `maxAttempts` stops being picked up, and a duplicate
after TTL expiry runs again.

## Used by

[signalpipe](https://github.com/sparkYJO1/signalpipe) — an ingest pipeline
built around a slow, expensive, non-deterministic consumer. The relay and the
guard came out of building it, which is why the API is shaped the way it is
rather than the way a library designed in the abstract would be.

## Built with Claude

Implemented by Claude against decisions I made and reviewed. The ADRs record
what an agent proposed and I rejected — the `LISTEN`/`NOTIFY` wake-up and
Postgres-backed deduplication both came from there — because supervising an
agent and following one are different jobs, and only one of them is worth
hiring.

## Licence

MIT.
