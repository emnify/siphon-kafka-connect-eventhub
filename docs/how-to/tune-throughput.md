# How to tune throughput

Work through these in order. The first two matter far more than the rest.

## 1. Do not defeat batching

The connector packs records into `EventDataBatch` objects and sends whole batches. Two settings destroy that:

- `eventhub.partition.key.source=record.key` with high-cardinality keys — batching becomes per key. See
  [Preserve per-key ordering](preserve-record-ordering.md).
- A very low `consumer.override.max.poll.records` — fewer records per `put()` means smaller batches.

Check the connector's debug log to see what you are actually getting:

```
DEBUG Uploaded 500 records in 3 batches
```

500 records in 3 batches is healthy. 500 records in 500 batches means batching is not happening.

## 2. Scale tasks before scaling clients

```properties
tasks.max=8
```

`tasks.max` is capped by the number of partitions across the source topics. Eight tasks over an eight-partition
topic is the ceiling; more tasks just sit idle.

Then, if a single task is connection-bound rather than CPU-bound:

```properties
eventhub.clients.per.task=2
```

Each client is one AMQP connection. Total connections are `tasks.max x eventhub.clients.per.task` — check that
against your namespace's concurrent-connection limit before raising either.

## 3. Give Event Hubs enough capacity

Throughput units (standard tier) or processing units (premium) cap namespace ingress. Exceeding them produces
`SERVER_BUSY_ERROR`, which the connector retries — you will see latency, not errors, until the retry budget runs
out.

Enable **auto-inflate** on the namespace so bursts do not become throttling.

Per-partition limits apply too: 1 MB/s or 1000 events/s per Event Hubs partition. If throughput is capped well
below your TU allocation, you probably have too few Event Hubs partitions, or skew from a hot partition key.

## 4. Raise the poll size

```properties
consumer.override.max.poll.records=1000
consumer.override.fetch.min.bytes=65536
```

Larger polls make fuller batches. The cost is a wider duplicate window on retry, and a longer `put()` — keep an
eye on the [timeout nesting](../explanation/retry-model.md).

## 5. Shrink the payload

The Event Hubs quota is per message *and* per byte. For schema'd records:

```properties
json.skip.null=true
```

which is the default, and omits null fields entirely.

If you control the producer, `ByteArrayConverter` end to end avoids the JSON conversion altogether — the connector
passes `byte[]` through untouched.

## 6. Transport

`AMQP_WEB_SOCKETS` adds framing overhead over plain `AMQP`. Only use it if a firewall forces you to — see
[Connect through a firewall or proxy](connect-through-a-firewall.md).

## Measuring

Kafka Connect's own JMX metrics are the honest source:

- `kafka.connect:type=sink-task-metrics,*` → `sink-record-send-rate`, `put-batch-avg-time-ms`
- `kafka.consumer:type=consumer-fetch-manager-metrics,*` → `records-lag-max`

A rising `records-lag-max` with a flat `sink-record-send-rate` means Event Hubs is the bottleneck, not the
connector. A high `put-batch-avg-time-ms` with low lag means you are over-provisioned.
