# ADR-0001 — Poll the table, do not tail the WAL

## Problem

The relay has to notice new outbox rows. Two ways: poll the table, or read
Postgres logical replication and react to the write.

CDC is the answer people reach for, and on paper it wins: no polling interval,
no wasted queries, lower latency.

## Decision

Poll, with `FOR UPDATE SKIP LOCKED` and an adaptive interval — zero wait while a
pass returns a full batch, otherwise a short sleep.

## Why, and this is the part that is actually about context

Logical replication is not free to operate. It needs a replication slot, and a
slot that stops being consumed **holds WAL forever** until the disk fills. It
needs `wal_level=logical`, which is a restart. It needs monitoring for slot lag
that is separate from everything else you monitor. On managed Postgres it may
need a plan tier you are not on.

I run a five-person team with no dedicated infrastructure engineer. The failure
mode of CDC here is not "slightly higher latency" — it is a disk filling at
3am from a slot nobody knew existed, taking the primary with it. Polling's
failure mode is that messages are published a few hundred milliseconds later
than they could have been.

That trade is only obvious once you say who is on call.

## Rejected

**`LISTEN`/`NOTIFY` as a wake-up.** Genuinely tempting: keep polling as the
floor, use a notification to skip the wait. Rejected for now because
`NOTIFY` is not delivered to a client that is not connected at the time, so
polling has to stay correct on its own anyway — and once it is correct on its
own, the notification is an optimisation, not a design. Worth adding later,
not worth the extra connection to get started.

**Long polling intervals to reduce load.** The query is indexed on exactly the
rows it reads (`WHERE published_at IS NULL`), so an empty pass is cheap. Tuning
the interval up trades latency for savings that do not exist.

## Consequence

Publish latency is bounded by the interval, 500ms by default. Under backlog it
is effectively zero, because a full batch skips the sleep.

`SKIP LOCKED` means more than one relay can run without coordination — each
takes rows the others are not holding. Ordering is preserved within a pass by
`ORDER BY id`, but two relays running concurrently can interleave across
passes. If strict global ordering ever matters, run one relay; per-key ordering
survives either way because the broker partitions on key.

## When to revisit

If publish latency becomes a product requirement rather than an implementation
detail, or if someone joins who owns the database full time. Both change the
inputs to this decision, not the reasoning.
