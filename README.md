# Kafka Connect Azure Event Hubs

A Kafka Connect **sink** connector that streams records from Kafka topics into Azure Event Hubs.

```bash
mvn package
```

- **Documentation:** [`docs/`](docs/README.md) — organised as [Diátaxis](https://diataxis.fr/):
  [tutorial](docs/tutorials/01-first-sink-connector.md) ·
  [how-to guides](docs/README.md#how-to-guides) ·
  [reference](docs/reference/configuration.md) ·
  [explanation](docs/README.md#explanation)
- **Upgrading from an older build:** [upgrade guide](docs/how-to/upgrade-from-v3.md) — contains breaking changes.
- **Open findings:** [`IMPROVEMENTS.md`](IMPROVEMENTS.md)

## At a glance

| | |
|---|---|
| Connector class | `com.microsoft.azure.eventhubs.kafka.connect.sink.EventHubSinkConnector` |
| Kafka Connect API | 3.7.2 |
| Event Hubs SDK | `com.azure:azure-messaging-eventhubs` 5.21.6 |
| Java | 11 or later |
| Delivery semantics | at-least-once — [duplicates are possible](docs/explanation/delivery-semantics.md) |
| Authentication | SAS connection string, or a JWT read from the filesystem |
| Version scheme | `<kafka-version>-<eventhubs-sdk-version>` |

## Minimal configuration

```properties
name=eventhub-sink
connector.class=com.microsoft.azure.eventhubs.kafka.connect.sink.EventHubSinkConnector
tasks.max=1
topics=events
eventhub.connection.string=Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>;EntityPath=<event-hub>
```

Before production, read
[Survive intermittent Event Hubs errors](docs/how-to/handle-intermittent-errors.md) — Kafka Connect's
`errors.retry.timeout` defaults to `0`, which disables batch-level retry entirely.

## How records are mapped

- Records **with a schema** (`Struct`) are serialized to JSON — the payload only, never Connect's
  `{"schema":..,"payload":..}` envelope.
- Records **without a schema** (`byte[]`, `ByteBuffer`, `String`) are passed through unchanged.
- **Tombstones** (null values) are skipped.
- Anything else is rejected as a poison record and
  [dead lettered](docs/how-to/dead-letter-poison-records.md).

Full table: [record to event mapping](docs/reference/data-mapping.md).

## Reliability in one paragraph

Transient Event Hubs failures — throttling, link detaches, connection recycles — are retried, first inside the
SDK and then by Kafka Connect. Configuration failures — a rejected credential, a missing Event Hub, an oversized
event — fail the task immediately with a tokenised, actionable message rather than burning the retry budget on a
result that cannot change. The connector proves its connection during `start()`, so a misconfiguration surfaces at
deploy time and not on the first record. See [fail fast or retry](docs/explanation/fail-fast-vs-retry.md).

## Building and testing

```bash
mvn test      # 117 unit tests, no network access required
mvn package   # connector jar + lib/, and a shaded jar
```

Dependency CVEs are scanned separately, not on every build:

```bash
mvn -B verify -Psecurity-scan   # OWASP dependency-check, fails at CVSS >= 7.0
```

It also runs weekly in CI. See [scan dependencies for known CVEs](docs/how-to/scan-dependencies-for-cves.md).

## Licence

[MIT](LICENSE).
