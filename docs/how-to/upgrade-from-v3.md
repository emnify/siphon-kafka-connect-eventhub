# How to upgrade from the v3 SDK build

This release replaces the retired `com.microsoft.azure:azure-eventhubs` 3.3.0 with
`com.azure:azure-messaging-eventhubs` 5.21.6, and moves the Kafka Connect API from 2.7.0 to 3.7.2.

Version scheme: `<kafka-version>-<eventhubs-sdk-version>`, so `2.7.0-3.5.0` becomes `3.7.2-5.21.6`.

## Breaking changes

### 1. `Date` logical types are now formatted strings

`org.apache.kafka.connect.data.Date` fields used to be emitted as an **integer** — days since the epoch — because
the logical converter's result fell through to the INT32 case. They are now emitted as a formatted date string.

```diff
- {"createdOn": 18417}
+ {"createdOn": "2020-06-04"}
```

This is a wire-format change for anything consuming those events. If a downstream consumer parses the integer,
update it before deploying, or keep the old shape by removing the `Date` logical name from the producer's schema.

`json.date.pattern` now actually applies and can control the format.

### 2. `Time` logical types used to crash

Any record containing an `org.apache.kafka.connect.data.Time` field threw `DateTimeException` and failed the task.
They now serialize as a time of day. If you were avoiding `Time` fields because of this, you no longer need to.

### 3. `json.date.pattern` and `json.datetime.pattern` were silently ignored

Both were defined and parsed, but never applied. If your configuration sets them, **the output format will change
on upgrade** to whatever you asked for. Check them before deploying.

An invalid pattern now fails the connector at startup rather than being ignored.

### 4. JWT authentication never worked

`eventhub.authentication=JWT` compared the configured value with `==` against a string constant. Values from a
parsed config map are not interned, so the comparison was always false and the connector silently used SAS.

If you configured JWT and it appeared to work, you were authenticating with the connection string's shared access
key. On upgrade the connector will genuinely use the JWT — make sure the token file exists and is valid first.
See [Authenticate with a filesystem JWT](authenticate-with-filesystem-jwt.md).

The token file layout is unchanged: `<dir>/<namespace>/<entityPath>`.

### 5. `CONNECT_AUTH_TOKEN_DIR` is now a property

The environment variable still works as a fallback, but prefer `eventhub.auth.token.dir` — it is visible in the
REST API.

### 6. Kafka Connect 3.x runtime required

Built against Connect API 3.7.2, deliberately: `connect-api` is a `provided` dependency, so it must match the
*lowest* runtime the plugin has to run on rather than the newest available. EMB runs
`confluentinc/cp-kafka-connect:7.7.7`, and Confluent Platform 7.7.x ships Apache Kafka 3.7.x. A plugin compiled
against a newer Connect API than its runtime provides fails with `NoSuchMethodError`; one compiled against 3.7
runs on 3.9 and 4.x as well.

It will not load on a Connect 2.x worker. The
`META-INF/services/org.apache.kafka.connect.sink.SinkConnector` entry is now present, which Kafka 4.x plugin
discovery requires.

### 7. `apache-log4j-extras` removed

It was declared but never used, and log4j 1.x is end-of-life. If your worker relied on the connector dragging it
onto the classpath, add it to the worker instead.

## New configuration

| Property | Default | Purpose |
|---|---|---|
| `eventhub.client.retry.mode` | `EXPONENTIAL` | Backoff shape. |
| `eventhub.client.try.timeout` | `30` | Per-attempt timeout. Previously the SDK default of 60s, not configurable. |
| `eventhub.send.timeout` | `240` | Overall `put()` deadline. Previously unbounded. |
| `eventhub.transport.type` | `AMQP` | `AMQP_WEB_SOCKETS` for port 443. |
| `eventhub.partition.key.source` | `none` | `record.key` to preserve per-key ordering. |
| `eventhub.auth.token.dir` | `$CONNECT_AUTH_TOKEN_DIR` | Filesystem JWT directory. |
| `json.time.pattern` | `""` | Format for `Time` logical types. |
| `json.timestamp.zone` | `UTC` | Zone for `Timestamp` logical types. |

Defaults preserve the previous behaviour except where noted above.

## Behavioural improvements you do not have to configure

- **Batching.** One AMQP send per record became one send per batch. Expect a large throughput increase and a drop
  in Event Hubs message count.
- **Retriable errors.** A transient failure now raises `RetriableException` instead of killing the task — but only
  takes effect if you set `errors.retry.timeout`, which defaults to `0`. See
  [Survive intermittent Event Hubs errors](handle-intermittent-errors.md).
- **Dead letter queue.** Unconvertible records can be diverted instead of failing the task.
- **Secrets.** The connection string is no longer logged at INFO on every task start.
- **Thread hygiene.** The shared, never-shut-down static executor is gone.

## Upgrade procedure

1. Read the breaking changes above and check your connector config against them.
2. Deploy the new plugin directory alongside the old one and restart the worker.
3. Update the connector's `connector.class` — unchanged — and add `errors.retry.timeout`.
4. Watch for the startup warning about timeout nesting.
5. Confirm `GET /connector-plugins` reports version `3.7.2-5.21.6`.

Roll back by restoring the previous plugin directory. There is no persistent state to migrate; Connect offsets are
unaffected.
