# Configuration properties

All properties are set on the connector, either in a `.properties` file for a standalone worker or in the `config`
object of a `POST /connectors` request.

Properties inherited from Kafka Connect itself (`name`, `topics`, `tasks.max`, `key.converter`, `value.converter`,
`errors.*`) are documented by [Kafka](https://kafka.apache.org/documentation/#sinkconnectconfigs) and are not
repeated here, except where this connector's behaviour depends on them.

## Connection

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `eventhub.connection.string` | password | *(required)* | high | Event Hubs connection string, including `EntityPath`. Masked in the REST API and in logs. |
| `eventhub.authentication` | string | `SAS` | high | `SAS` to authenticate with the connection string's shared access key, or `JWT` to read a token from the filesystem. |
| `eventhub.auth.token.dir` | string | `$CONNECT_AUTH_TOKEN_DIR` | medium | Directory holding filesystem JWTs. Only used when `eventhub.authentication=JWT`. The token is read from `<dir>/<namespace>/<entityPath>`. |
| `eventhub.transport.type` | string | `AMQP` | medium | `AMQP` (TCP port 5671) or `AMQP_WEB_SOCKETS` (TCP port 443). |
| `eventhub.clients.per.task` | short | `1` | high | Number of producer clients, and therefore AMQP connections, per task. Minimum 1. |
| `eventhub.validate.on.start` | boolean | `true` | medium | Prove the connection during `start()` so a bad credential or a missing Event Hub fails the task immediately rather than on the first record. |
| `eventhub.start.timeout` | long (s) | `60` | low | How long `start()` retries a **transient** startup failure. Configuration faults are never retried, however large this is. |

## Reliability

See [The two-layer retry model](../explanation/retry-model.md) for how these interact.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `eventhub.client.retry.count` | int | `3` | low | Retries the SDK performs on a transient failure before surfacing it to the task. `0` disables in-client retry. |
| `eventhub.client.retry.mode` | string | `EXPONENTIAL` | low | `EXPONENTIAL` or `FIXED` backoff. |
| `eventhub.client.retry.minimumBackoff` | long (s) | `1` | low | Initial backoff between in-client retries. |
| `eventhub.client.retry.maximumBackoff` | long (s) | `30` | low | Backoff ceiling. Clamped up to `minimumBackoff` if configured lower. |
| `eventhub.client.try.timeout` | long (s) | `30` | medium | Timeout of a single send attempt, before the SDK retries it. |
| `eventhub.send.timeout` | long (s) | `240` | medium | Overall deadline for one `put()` to deliver its batch. Must stay below the worker's `consumer.max.poll.interval.ms`. |

The defaults nest deliberately:

```
client worst case   (3 + 1) x 30s  +  3 x 30s backoff   =  210s
eventhub.send.timeout                                   =  240s
consumer.max.poll.interval.ms (Kafka default)           =  300s
```

The task logs a warning at startup if a custom configuration inverts that ordering.

## Routing

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `eventhub.partition.key.source` | string | `none` | medium | `none` lets Event Hubs distribute events across partitions. `record.key` uses the Kafka record key as the Event Hubs partition key. See [Preserve per-key ordering](../how-to/preserve-record-ordering.md). |

## Serialization

Applies to records that carry a Connect schema. Records whose value is already `byte[]`, `ByteBuffer` or `String`
are passed through untouched.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `eventhub.serialization` | string | `json` | low | Only `json` is implemented. |
| `json.skip.null` | boolean | `true` | low | Omit null struct fields and null map values from the output instead of emitting `null`. Nulls inside arrays are always kept, because dropping them would shift the index of every following element. |
| `json.date.pattern` | string | `""` | low | `DateTimeFormatter` pattern for `Date` logical types. Empty means `ISO_LOCAL_DATE`. |
| `json.datetime.pattern` | string | `""` | low | `DateTimeFormatter` pattern for `Timestamp` logical types. Empty means `ISO_LOCAL_DATE_TIME`. |
| `json.time.pattern` | string | `""` | low | `DateTimeFormatter` pattern for `Time` logical types. Empty means `ISO_LOCAL_TIME`. |
| `json.timestamp.zone` | string | `UTC` | low | Zone used to render `Timestamp` logical types. Changing it changes the emitted values. |
| `json.decimal.format` | string | `BASE64` | low | How `Decimal` logical types are rendered: `BASE64` (the unscaled two's-complement bytes as a base64 string, matching Connect's own `JsonConverter`) or `NUMERIC` (a JSON number). Case-insensitive; any other value is rejected at startup. |

An invalid pattern or zone fails the connector at startup, naming the offending property, rather than on the first
record that happens to use it.

## Connect properties worth setting

| Property | Suggested | Why |
|---|---|---|
| `errors.tolerance` | `all` | Required for the dead letter queue to be used at all. Without it a single unconvertible record fails the task. |
| `errors.deadletterqueue.topic.name` | *(a topic)* | Where unconvertible records go. See [Dead letter poison records](../how-to/dead-letter-poison-records.md). |
| `errors.deadletterqueue.context.headers.enable` | `true` | Adds the original topic, partition, offset and exception to the dead letter record's headers. |
| `errors.retry.timeout` | `-1` or a generous value | How long Connect keeps re-delivering a batch that raised a retriable failure. `0`, the default, means a transient Event Hubs error fails the task on the first occurrence. |
| `errors.retry.delay.max.ms` | `60000` | Backoff ceiling between Connect-level retries. |
| `consumer.override.max.poll.records` | `500` (default) | Upper bound on the records one `put()` has to batch and send. |
