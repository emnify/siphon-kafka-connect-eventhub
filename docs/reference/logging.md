# Logging

The connector logs through SLF4J, which the Connect worker binds. It ships no logging backend of its own, and
`slf4j-api` is explicitly excluded from the packaged artifacts so it cannot shadow the worker's.

## Loggers

| Logger | Emits |
|---|---|
| `com.microsoft.azure.eventhubs.kafka.connect.sink.EventHubSinkConnector` | connector lifecycle, task config fan-out |
| `...EventHubSinkTask` | task lifecycle, resolved configuration, batch send outcomes |
| `...EventHubProducerProvider` | one line per producer opened |
| `...FilesystemTokenCredential` | token file path, token loads, malformed-token warnings |
| `com.azure.messaging.eventhubs`, `com.azure.core.amqp` | the SDK's own AMQP diagnostics |

Turn up the SDK's loggers, not the connector's, when diagnosing a connection problem:

```properties
log4j.logger.com.azure.core.amqp=DEBUG
```

## Lines worth alerting on

| Level | Message | Meaning |
|---|---|---|
| `WARN` | `... is below the client's worst-case retry budget of ...` | The timeouts are mis-nested. Fix before it bites. See [the retry model](../explanation/retry-model.md). |
| `WARN` | `Transient failure sending to Event Hubs for batch[...], the batch will be retried` | Normal under load. A sustained rate means Event Hubs is throttling. |
| `WARN` | `Dead lettering unconvertible record [...]` | A poison record was diverted. A sustained rate means a producer is emitting bad data. |
| `ERROR` | `Permanent failure sending to Event Hubs for batch[...]` | The task is stopping. Page someone. |
| `WARN` | `Token in ... has no 'exp' claim` | The JWT sidecar is writing something unexpected. |
| `WARN` | `Event Hubs send threads did not terminate within 30s` | A send was stuck at shutdown. |

## What is never logged

The connection string, and the JWT itself. The connector logs the namespace and Event Hub name instead:

```
INFO Opening Event Hubs producer [namespace=ns.servicebus.windows.net, eventHub=events, auth=SAS, transport=AMQP]
```

`eventhub.connection.string` is declared as a `PASSWORD` property, so Connect masks it in the REST API and in its
own configuration logging too.

Record values are never included in exception messages, because they routinely carry personal data. If you need
them while debugging, Connect can add them with `errors.log.include.messages=true` — turn it off again afterwards.

## Reading a batch failure

```
batch[events=200, bytes=48213, partitionKey=null, from=events-3@81234, to=events-3@81433]
```

| Field | Meaning |
|---|---|
| `events` | events in the batch |
| `bytes` | encoded size |
| `partitionKey` | Event Hubs partition key, or `null` |
| `from` / `to` | Kafka `topic-partition@offset` of the first and last record |

`from` and `to` are what you feed to a console consumer to see exactly which records were affected.

## Debug logging

```properties
log4j.logger.com.microsoft.azure.eventhubs.kafka.connect.sink=DEBUG
```

Adds one line per `put()`:

```
DEBUG Uploading 500 records
DEBUG Uploaded 500 records in 3 batches
```

The ratio of records to batches is the quickest check that batching is working. See
[Tune throughput](../how-to/tune-throughput.md).
