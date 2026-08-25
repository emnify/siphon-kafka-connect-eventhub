# Stream your first topic to an Event Hub

By the end of this tutorial you will have a Kafka Connect worker copying records from a Kafka topic into an Azure
Event Hub, and you will have watched an event arrive on the other side.

This is a learning exercise on a single machine. It is not a production setup — see
[Build and deploy](../how-to/build-and-deploy.md) for that.

## What you need

- JDK 11 or later, and Maven
- A local Kafka distribution (3.x or 4.x), unpacked
- An Azure Event Hubs namespace with one Event Hub, and a shared access policy that has the **Send** claim

## 1. Build the connector

```bash
git clone https://github.com/emnify/siphon-kafka-connect-eventhub.git
cd siphon-kafka-connect-eventhub
mvn package
```

You should see `BUILD SUCCESS` and, in `target/`, a connector jar plus a `lib/` directory of its dependencies.

## 2. Install it as a plugin

Kafka Connect discovers plugins as directories under `plugin.path`. Make one:

```bash
mkdir -p /tmp/connect-plugins/eventhub-sink
cp target/azure-eventhub-connector-*.jar /tmp/connect-plugins/eventhub-sink/
cp -r target/lib /tmp/connect-plugins/eventhub-sink/
```

## 3. Start Kafka

In three separate terminals, from your Kafka distribution directory:

```bash
# terminal 1
bin/zookeeper-server-start.sh config/zookeeper.properties

# terminal 2
bin/kafka-server-start.sh config/server.properties

# terminal 3
bin/kafka-topics.sh --create --topic events --bootstrap-server localhost:9092
```

(On Kafka 4.x, which has no ZooKeeper, follow the KRaft quickstart instead and skip terminal 1.)

## 4. Configure the worker

Create `/tmp/connect-worker.properties`:

```properties
bootstrap.servers=localhost:9092
offset.storage.file.filename=/tmp/connect.offsets
plugin.path=/tmp/connect-plugins

key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
```

Both converters are `StringConverter`, so your record values arrive at the connector as `String` and are sent to
Event Hubs as UTF-8 bytes. No schema, no JSON conversion — the simplest possible path.

## 5. Configure the connector

Create `/tmp/eventhub-sink.properties`. Take the connection string from the Azure Portal, under
**Event Hubs → your namespace → Shared access policies → your policy → Connection string–primary key**, and append
`;EntityPath=<your-event-hub-name>` if it is not already there.

```properties
name=eventhub-sink
connector.class=com.microsoft.azure.eventhubs.kafka.connect.sink.EventHubSinkConnector
tasks.max=1
topics=events

eventhub.connection.string=Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>;EntityPath=<event-hub>
```

## 6. Start the connector

```bash
bin/connect-standalone.sh /tmp/connect-worker.properties /tmp/eventhub-sink.properties
```

Look for these lines:

```
INFO Starting EventHubSinkConnector 3.7.2-5.21.6
INFO EventHubSinkTask configuration [clientsPerTask=1, auth=SAS, transport=AMQP, ...]
INFO Opening Event Hubs producer [namespace=<namespace>.servicebus.windows.net, eventHub=<event-hub>, ...]
INFO Opened 1 Event Hubs producer(s)
```

The connection string is never logged — only the namespace and Event Hub name. That is deliberate: it carries your
shared access key.

## 7. Send something

In another terminal:

```bash
bin/kafka-console-producer.sh --topic events --bootstrap-server localhost:9092
```

Type a few lines and press Enter after each:

```
hello event hubs
second event
```

## 8. Confirm it arrived

In the Azure Portal, open your Event Hub and look at the **Overview** chart — the *Messages* count rises within a
minute or so.

To see the actual bodies, use **Data Explorer** on the Event Hub blade, select a partition, and choose to view
events from the start. You should find `hello event hubs`.

## 9. Watch a retry happen

Stop the connector (Ctrl-C). Break the connection string by changing one character of the `SharedAccessKey`, and
start it again.

```
ERROR Unable to open Event Hubs producers [namespace=..., eventHub=...]:
      AmqpException[condition=UNAUTHORIZED_ACCESS, transient=true]: ...
```

The task fails rather than retrying, even though the SDK marked the error transient. A rejected credential will be
rejected identically on every attempt, so retrying it would only waste the retry budget. That judgement is what
the [error classification table](../reference/error-classification.md) records.

Fix the key and restart — the task recovers.

## Where to go next

- [Survive intermittent Event Hubs errors](../how-to/handle-intermittent-errors.md) — the settings you need before
  this goes anywhere near production.
- [Configuration properties](../reference/configuration.md) — everything you can tune.
- [Delivery semantics](../explanation/delivery-semantics.md) — why duplicates are possible and what to do about it.
