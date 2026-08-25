# How to preserve per-key ordering

**Default behaviour: no ordering is preserved at all.** Event Hubs distributes events across partitions, and a
single `put()` sends its batches concurrently, so even records from one Kafka partition can be reordered.

If that matters, route by key.

## Enable it

```properties
eventhub.partition.key.source=record.key
```

All events sharing a key are packed into one batch and sent with that Event Hubs partition key. Event Hubs hashes
the key to a partition, and preserves order within a partition.

## What you get

- Records with the same Kafka key arrive at the same Event Hubs partition, in order.
- Records with different keys have no ordering relationship. That is unchanged.
- Records with a null or empty key get no partition key, and are distributed as before.

## What it costs

**Throughput.** Batching is per key. A topic with 10,000 distinct keys in a `put()` of 500 records produces up to
500 single-event batches instead of one or two full ones — the connector degrades to roughly one AMQP send per
record. Key-based routing suits low-cardinality keys (tenant id, device group), not high-cardinality ones
(request id, user id).

**Partition skew.** Event Hubs hashes the key; a hot key makes a hot partition. A single Event Hubs partition has
a hard ingress limit of 1 MB/s or 1000 events/s. Kafka's own partitioning has the same property, so if your Kafka
partitions are already balanced by this key, the Event Hubs ones will be too.

## Ordering is still not guaranteed end-to-end

Two caveats survive:

1. **Retries reorder.** If a `put()` fails partway and is retried, the replayed batches are re-sent after batches
   from the following `put()` may already have been accepted. Kafka Connect's contract does not prevent this.
2. **Duplicates.** The connector is [at-least-once](../explanation/delivery-semantics.md). An ordered stream with
   duplicates is not the same as an ordered stream.

If you need a strict per-key sequence downstream, carry a sequence number in the event body and let the consumer
reorder or discard. Partition-key routing makes that cheap because everything for a key is in one partition; it
does not make it unnecessary.

## Key types

See the [record to event mapping](../reference/data-mapping.md) for how each Kafka key type becomes a partition
key. `byte[]` keys are decoded as UTF-8 — if your keys are not UTF-8 text, they will produce a stable but
unreadable partition key.
