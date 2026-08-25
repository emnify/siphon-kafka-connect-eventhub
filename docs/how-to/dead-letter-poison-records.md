# Dead letter poison records

**Goal:** keep the task running when a single record cannot be converted, instead of failing the connector for
everyone behind it.

A *poison record* is one this connector can never send, no matter how many times it retries — a value whose type
does not match its schema, a required field that is null, or an event larger than the Event Hub's maximum message
size. Retrying it is pointless; it will fail identically forever.

By default there is no dead letter queue, and a poison record **fails the task**. That is deliberate: silently
dropping a record would be data loss the operator never hears about.

## Turn the dead letter queue on

Both properties are required. `errors.deadletterqueue.topic.name` alone does nothing without
`errors.tolerance=all`.

```properties
errors.tolerance=all
errors.deadletterqueue.topic.name=eventhub-sink-dlq
errors.deadletterqueue.context.headers.enable=true
errors.deadletterqueue.topic.replication.factor=3
```

These are Kafka Connect properties, not connector properties — they are handled by the framework, and are
documented in full under [sink connector configs](https://kafka.apache.org/documentation/#sinkconnectconfigs).

`errors.deadletterqueue.topic.replication.factor` defaults to `3` and the topic is created automatically. On a
single-broker development cluster set it to `1`, or the DLQ topic cannot be created and the task fails on the
first bad record.

## What lands there

Only conversion failures. The record is routed to the DLQ when:

- the value cannot be converted to JSON — a `DataException` from the formatter, such as a `Struct` whose value
  does not match its schema, or a required field holding null;
- the resulting event does not fit in an empty batch, meaning it exceeds the Event Hub's maximum message size and
  can never be sent.

Everything else keeps its usual outcome. A transient Event Hubs failure is retried, and a configuration or
authorization failure fails the task — those are not the record's fault, and dead lettering them would quietly
discard good data. See the [error classification table](../reference/error-classification.md).

## Read the failure

With `errors.deadletterqueue.context.headers.enable=true` each dead letter record carries the original topic,
partition, offset and exception in its headers, prefixed `__connect.errors.`:

```bash
kcat -b "$BOOTSTRAP" -t eventhub-sink-dlq -C -f '%h\n%s\n\n' -o -5 -e
```

The connector also logs one line per dead lettered record, at `WARN`, naming the coordinates but never the value
itself:

```
Dead lettering unconvertible record [topic=cdr, partition=3, offset=91827]: Invalid type for STRING: class java.lang.Integer
```

Record values are deliberately kept out of both the log and the exception message — they are customer data, and a
stack trace is not a safe place for it.

## When there is no dead letter queue

If the runtime supports a reporter but none is configured, the task fails with a message naming what to set:

```
Unable to convert a record and no dead letter queue is configured (set errors.tolerance=all and
errors.deadletterqueue.topic.name to keep the task running) [topic=cdr, partition=3, offset=91827]
```

On a Connect runtime older than 2.6 there is no `ErrantRecordReporter` at all. The connector detects this at
startup and logs it once:

```
This Connect runtime does not support a dead letter queue for sink connectors
```

On such a runtime a poison record always fails the task, whatever `errors.tolerance` says.

## Do not leave it unwatched

A dead letter queue turns a loud failure into a quiet one. That is the point, but it means nothing tells you the
records are piling up. Alert on the DLQ topic's message rate, or on the connector's `WARN` lines, and treat a
non-empty DLQ as a bug to fix rather than a bin to ignore.

## See also

- [Error classification table](../reference/error-classification.md) — which failures are retried, which fail the
  task, and which are dead lettered.
- [Record to event mapping](../reference/data-mapping.md) — the conversion rules that produce these failures.
- [Survive intermittent Event Hubs errors](handle-intermittent-errors.md) — the retry side of the same story.
