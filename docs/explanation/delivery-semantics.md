# Delivery semantics

**This connector is at-least-once. Downstream consumers must tolerate duplicates.**

## Why not exactly-once

Exactly-once from Kafka to an external system needs one of two things: an idempotent write (the destination
recognises and discards a replay) or a transactional write joined to the offset commit. Azure Event Hubs offers
neither to a producer. The idempotent-producer feature exists only for a single partition with a single exclusive
producer, which is incompatible with `tasks.max > 1` and with partition-key routing.

## Where duplicates come from

`put()` receives a batch of records, packs them into one or more Event Hubs batches, and sends them concurrently.
It returns only when all of them are acknowledged.

If any batch fails transiently, the task raises `RetriableException` and Connect re-delivers **the entire
`put()` batch** — including the Event Hubs batches that were already acknowledged. Those events are sent twice.

```
put([r1..r500])
  batch A (r1..r250)  -> acknowledged
  batch B (r251..r500) -> SERVER_BUSY after the SDK's retries
  => RetriableException

put([r1..r500])     <- Connect re-delivers all 500
  batch A (r1..r250)  -> acknowledged AGAIN     <-- duplicates
  batch B (r251..r500) -> acknowledged
```

The window is bounded by the size of one `put()`, which is bounded by `consumer.override.max.poll.records`
(500 by default). Lowering it narrows the duplicate window at the cost of throughput.

A second, smaller source: if `eventhub.send.timeout` elapses, the task abandons the outstanding sends and raises
`RetriableException`. Those sends are *not* actually cancelled — `CompletableFuture.cancel` cannot interrupt the
worker thread — so a send that completes after the deadline is a duplicate on the next attempt.

## Why offsets are still safe

`put()` blocks until Event Hubs has acknowledged everything, and `flush()` is empty. Connect only commits an
offset after the corresponding `put()` returned successfully, so a committed offset always corresponds to data
that reached Event Hubs. Records are never *lost*, only occasionally repeated.

## Making it safe downstream

- Give each event an idempotency key the consumer can deduplicate on. If the Kafka record has a meaningful key,
  set `eventhub.partition.key.source=record.key` so duplicates land in the same Event Hubs partition, which makes
  a windowed deduplication cheap.
- Prefer downstream operations that are naturally idempotent — upserts keyed on a business id rather than inserts.
- If the consumer is a stream processor, deduplicate on `(topic, partition, offset)` carried as an application
  property. That requires header propagation, which is on the [improvement backlog](../../IMPROVEMENTS.md).

## Ordering

Without a partition key, Event Hubs distributes events across partitions and **no ordering is preserved at all** —
not even within a Kafka partition, because one `put()` sends its batches concurrently.

With `eventhub.partition.key.source=record.key`, all events sharing a key go to one Event Hubs partition in one
batch, and Event Hubs preserves order within a partition. See
[Preserve per-key ordering](../how-to/preserve-record-ordering.md) for the caveats.
