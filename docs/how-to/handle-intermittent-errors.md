# How to survive intermittent Event Hubs errors

**Goal:** the connector shrugs off throttling, link detaches and brief network faults instead of dying and waiting
for a human.

Background: [The two-layer retry model](../explanation/retry-model.md).

## 1. Turn on Connect-level retry — this is the one everybody misses

`errors.retry.timeout` defaults to `0`, which means Kafka Connect performs **no** retry at all. The connector can
classify a failure as retriable perfectly and it will still kill the task.

```properties
errors.tolerance=all
errors.retry.timeout=600000
errors.retry.delay.max.ms=60000
```

`errors.retry.timeout=-1` retries forever. Prefer a bounded value with alerting on the task state, so a genuine
outage is visible rather than silently absorbed.

## 2. Give the SDK a realistic retry budget

Most intermittent failures should never reach Kafka Connect at all. Widen layer 1 first:

```properties
eventhub.client.retry.count=5
eventhub.client.retry.mode=EXPONENTIAL
eventhub.client.retry.minimumBackoff=1
eventhub.client.retry.maximumBackoff=20
eventhub.client.try.timeout=30
```

## 3. Keep the three timeouts nested

Layer 1's worst case must fit inside `eventhub.send.timeout`, which must fit inside
`consumer.max.poll.interval.ms`:

```
(5 + 1) x 30s  +  5 x 20s   =  280s      <-- too big for the 240s default
```

So raise the deadline, and raise Kafka's patience to match:

```properties
eventhub.send.timeout=290
consumer.override.max.poll.interval.ms=420000
```

Check the connector log at startup. If you got this wrong you will see:

```
WARN  eventhub.send.timeout=240s is below the client's worst-case retry budget of 280s ...
```

Ignoring that warning is how a throttling burst becomes a retry loop that never makes progress.

## 4. Reduce the blast radius of a retry

A Connect-level retry replays the whole `put()` batch, duplicates included. Smaller batches mean fewer duplicates:

```properties
consumer.override.max.poll.records=200
```

## 5. Deal with the actual cause of throttling

`SERVER_BUSY_ERROR` means you are exceeding the namespace's throughput units. Retrying only buys time. Either
raise the TU/PU allocation, enable auto-inflate on the namespace, or spread load over more producers:

```properties
tasks.max=4
eventhub.clients.per.task=2
```

Remember that this is `4 x 2 = 8` AMQP connections against the namespace's concurrent-connection limit.

## 6. Do not retry poison records

An unconvertible or oversized record will fail identically forever. Route it out of the way instead:

```properties
errors.deadletterqueue.topic.name=eventhub-sink-dlq
errors.deadletterqueue.topic.replication.factor=3
errors.deadletterqueue.context.headers.enable=true
```

See [Dead letter poison records](dead-letter-poison-records.md).

## A worked configuration

```properties
name=eventhub-sink
connector.class=com.microsoft.azure.eventhubs.kafka.connect.sink.EventHubSinkConnector
tasks.max=4
topics=events

eventhub.connection.string=${file:/opt/secrets/eventhub.properties:connection-string}
eventhub.clients.per.task=2

eventhub.client.retry.count=5
eventhub.client.retry.mode=EXPONENTIAL
eventhub.client.retry.minimumBackoff=1
eventhub.client.retry.maximumBackoff=20
eventhub.client.try.timeout=30
eventhub.send.timeout=290

errors.tolerance=all
errors.retry.timeout=600000
errors.retry.delay.max.ms=60000
errors.deadletterqueue.topic.name=eventhub-sink-dlq
errors.deadletterqueue.context.headers.enable=true
errors.log.enable=true

consumer.override.max.poll.records=200
consumer.override.max.poll.interval.ms=420000
```

## Verifying it works

Search the connector log for the two lines that matter.

A transient failure that was absorbed and retried:

```
WARN  Transient failure sending to Event Hubs for batch[events=200, bytes=..., partitionKey=null,
      from=events-3@81234, to=events-3@81433], the batch will be retried:
      AmqpException[condition=SERVER_BUSY_ERROR, transient=true]: ...
```

A permanent failure that correctly stopped the task rather than spinning:

```
ERROR Permanent failure sending to Event Hubs for batch[...]:
      AmqpException[condition=UNAUTHORIZED_ACCESS, transient=true]: ...
```

Note the second one: the SDK called it transient and the connector overruled it. That is deliberate — see the
[error classification table](../reference/error-classification.md).
