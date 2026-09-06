# ADR-0002 — At-least-once, and saying so

## Problem

Duplicate delivery is guaranteed by the broker's contract. The guard exists to
make the second delivery cheap. The question is what to promise about it.

## Decision

Deduplicate on message id in Redis with a TTL, default seven days. Reserve
before running, not after. Document the result as **at-least-once with a
deduplication window**, and never as exactly-once.

## The TTL is the whole trade, stated plainly

A duplicate arriving inside the window is suppressed. A duplicate arriving
after it **is processed again**. That is not a bug to be apologised for, it is
the shape of the design, and there is an integration test asserting it rather
than a comment hoping nobody notices.

Choosing the number is choosing what you are protecting against:

- **Redelivery after a consumer crash** — seconds to minutes. Any TTL works.
- **A partition replayed after an incident** — hours to days. Seven days covers
  most of it.
- **A full topic replay from the beginning** — nothing reasonable covers this,
  and a TTL long enough to try would be a memory leak with a schedule.

Seven days is chosen to cover the second case, which is the one that actually
happens. The third is handled by not doing it, or by accepting reprocessing and
making handlers tolerant.

## Rejected

**Claim exactly-once.** It is achievable in narrow setups — Kafka transactions
with a transactional sink — and this is not one of them, because the sink is
arbitrary handler code that can touch anything. Claiming it would be false, and
in an interview it is the claim that ends the conversation.

**Dedup in Postgres instead of Redis.** Correct and durable, and it puts a
write on the hot path of every message plus a table that grows forever and
needs pruning. Redis with a TTL prunes itself. The cost is that losing Redis
loses the window — which is the same shape as ADR-0003 in signalpipe: it costs
repeated work, never correctness, because handlers must be idempotent anyway.

**Mark after running instead of reserving first.** This is the common
implementation and it has a race: two concurrent deliveries both check, both
find nothing, both run. `SET NX` first closes it. There is a test for exactly
this, asserting the handler never runs concurrently for one id.

## Consequence

The guard fails **open**, not closed: if the handler throws, the reservation is
released so a retry is not swallowed as a duplicate. That means a crash between
handler success and the final `SET` can cause one extra run. Losing work is
worse than repeating it, so that is the direction to fail in — but it is a
choice, and reversing it would mean choosing the opposite.
