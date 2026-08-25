# How to build and deploy the connector

## Build

```bash
mvn package
```

Requires JDK 11 or later. The build targets Java 11 bytecode regardless of the JDK you build with
(`maven.compiler.release`).

Two artifacts are produced:

| Artifact | Use it when |
|---|---|
| `target/azure-eventhub-connector-<version>.jar` + `target/lib/` | The standard Connect plugin layout. Preferred. |
| `target/azure-eventhub-connector-<version>-jar-with-dependencies.jar` | You need a single file, e.g. baking into a container image layer. |

## Install as a Connect plugin

Kafka Connect scans `plugin.path` for directories, each containing one plugin and its dependencies.

```bash
mkdir -p /opt/kafka/plugins/eventhub-sink
cp target/azure-eventhub-connector-*.jar /opt/kafka/plugins/eventhub-sink/
cp -r target/lib /opt/kafka/plugins/eventhub-sink/
```

```properties
plugin.path=/opt/kafka/plugins
```

Do **not** put the connector on the worker's main classpath. Plugin isolation is what keeps the connector's
Jackson and Reactor versions from colliding with the worker's.

### Verify discovery

```bash
curl -s localhost:8083/connector-plugins | jq '.[] | select(.class | contains("EventHub"))'
```

```json
{
  "class": "com.microsoft.azure.eventhubs.kafka.connect.sink.EventHubSinkConnector",
  "type": "sink",
  "version": "3.7.2-5.21.6"
}
```

A `version` of `unknown` means the jar was built without its manifest — you are probably pointing at a classes
directory rather than the packaged jar.

## Create the connector

```bash
curl -X POST localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "eventhub-sink",
    "config": {
      "connector.class": "com.microsoft.azure.eventhubs.kafka.connect.sink.EventHubSinkConnector",
      "tasks.max": "4",
      "topics": "events",
      "eventhub.connection.string": "Endpoint=sb://...;EntityPath=hub",
      "errors.tolerance": "all",
      "errors.retry.timeout": "600000",
      "errors.deadletterqueue.topic.name": "eventhub-sink-dlq"
    }
  }'
```

The connection string is declared as a `PASSWORD` property, so `GET /connectors/eventhub-sink` returns it masked.

### Keep the secret out of the connector config

Use a Connect config provider rather than inlining the key:

```properties
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

```json
"eventhub.connection.string": "${file:/opt/secrets/eventhub.properties:connection-string}"
```

## Sizing tasks

`tasks.max` is capped by the number of partitions in the source topics. Total AMQP connections to the namespace
are `tasks.max x eventhub.clients.per.task`; check that against the namespace's concurrent-connection limit.

## Release

Releases are cut by the Jenkins pipeline in `.jenkins/Jenkinsfile`, which deploys to AWS CodeArtifact on merges to
`master`. The version scheme is `<kafka-version>-<eventhubs-sdk-version>`.
