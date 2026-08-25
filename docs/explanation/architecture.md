# Architecture

## What this connector is

A Kafka Connect **sink** connector. Kafka Connect owns the consumer group, the polling loop and the offset
commits; this connector owns everything from "here are some records" to "Event Hubs acknowledged them".

There is no source connector here. To read *from* Event Hubs, use its Kafka-protocol endpoint with an ordinary
Kafka consumer.

## Classes

```
EventHubSinkConnector      validates config, hands each task its own copy
      |
      v
EventHubSinkTask           the loop: convert -> batch -> send -> classify failures
      |
      +-- EventDataExtractor        SinkRecord value  -> EventData
      |         |
      |         +-- OutputJsonFormatter    Struct -> JSON tree
      |                   |
      |                   +-- OutputJsonSerializer   JSON tree -> bytes
      |
      +-- EventHubProducerProvider  config -> EventHubProducerClient
      |         |
      |         +-- EventHubConnectionString      parses namespace + entity path
      |         +-- FilesystemTokenCredential     JWT auth from a file on disk
      |
      +-- EventHubErrors            Throwable -> retriable / permanent
```

Each class is separately testable, which is why `EventHubProducerProvider` exists at all: it is the seam that lets
the task's tests run without reaching Azure.

## The lifecycle of a `put()`

1. **Convert.** Each record's value becomes an `EventData`. Tombstones are skipped. A value that cannot be
   converted raises `DataException` and is dead lettered — it will never convert, so it must not be retried.
2. **Batch.** Events are packed into `EventDataBatch` objects, one open batch per partition key. When a batch
   reports itself full it is sealed and a new one opened. An event that does not fit in an *empty* batch exceeds
   the Event Hub's maximum message size and is dead lettered.
3. **Send.** Batches are sent concurrently across the task's producer pool, on a fixed thread pool sized to the
   pool. Every send is awaited.
4. **Classify.** All failures are collected before anything is thrown. A permanent failure always beats a
   transient one, so a poison batch cannot hide behind a concurrent throttling response and retry forever.

## Why batching

The previous implementation issued one AMQP send per record: 500 records in a `put()` meant 500 round trips. Event
Hubs charges and rate-limits per *message*, and AMQP round trips dominate the cost. Packing the same records into
a handful of `EventDataBatch` sends is the largest single throughput difference in the connector.

## Why a pool of producers

Each `EventHubProducerClient` owns one AMQP connection. One connection has a finite link credit and a single TCP
socket. `eventhub.clients.per.task` producers give a task that many independent connections, and batches are
distributed across them round-robin.

More is not automatically better: each connection counts against the namespace's concurrent-connection limit, and
`tasks.max` multiplies it. Total connections are `tasks.max x eventhub.clients.per.task`.

## Threading

| Thread | Owns |
|---|---|
| Connect task thread | `start`, `put`, `flush`, `stop`. Blocks inside `put` until all sends are acknowledged. |
| `eventhub-sink-send-N` | One blocking `producer.send(batch)` each. Daemon threads, sized to the producer pool, created in `start` and shut down in `stop`. |
| SDK reactor threads | Owned by the Azure SDK. Not this connector's concern. |

The send threads are daemons and are shut down explicitly, because Connect isolates each plugin in its own
classloader: a non-daemon pool thread that outlives a deleted connector pins that classloader in memory forever.
The previous implementation used a `static ScheduledThreadPoolExecutor` that was shared across every connector in
the worker and never shut down.

## Plugin packaging

`mvn package` produces two things:

- `target/azure-eventhub-connector-<version>.jar` plus `target/lib/` — the standard Connect plugin layout. Copy
  both into one directory under `plugin.path`.
- `target/azure-eventhub-connector-<version>-jar-with-dependencies.jar` — a single self-contained jar.

Both include `META-INF/services/org.apache.kafka.connect.sink.SinkConnector`. Kafka 3.6 warns about plugins
discovered only by reflection, and Kafka 4 defaults `plugin.discovery` to `service_load`, which would not find
this connector without that file.
