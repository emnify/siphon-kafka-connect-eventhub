# How to authenticate with a SAS connection string

This is the default and needs no `eventhub.authentication` setting.

## Get the connection string

Azure Portal → **Event Hubs** → your namespace → **Shared access policies**.

Create a policy with only the **Send** claim. Do not use `RootManageSharedAccessKey` — it grants Manage and Listen
as well, and a sink connector needs neither.

Copy **Connection string–primary key**.

## Make sure it names the Event Hub

A namespace-level policy gives you:

```
Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=...
```

The connector needs to know which Event Hub, so append `EntityPath`:

```
Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=...;EntityPath=events
```

An entity-level policy (created on the Event Hub rather than the namespace) already includes `EntityPath`.

## Configure

```properties
eventhub.authentication=SAS
eventhub.connection.string=${file:/opt/secrets/eventhub.properties:connection-string}
```

## Rotating the key

The connector reads the connection string once, at task start. Rotating the shared access key requires restarting
the connector:

```bash
curl -X POST localhost:8083/connectors/eventhub-sink/restart?includeTasks=true
```

Rotate against the *secondary* key first, so there is a working key throughout:

1. Regenerate the secondary key.
2. Update the secret to use the secondary key, restart the connector, confirm it is `RUNNING`.
3. Regenerate the primary key.

If you need rotation without a restart, use [filesystem JWT authentication](authenticate-with-filesystem-jwt.md),
which re-reads the credential as it approaches expiry.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `AmqpException[condition=UNAUTHORIZED_ACCESS]` at startup | Wrong key, or the policy lacks the **Send** claim. |
| `AmqpException[condition=NOT_FOUND]` at startup | `EntityPath` names an Event Hub that does not exist. |
| `ConfigException: ... is missing the 'Endpoint' component` | The connection string was truncated, often by shell quoting. |

These are all classified permanent — the task fails immediately rather than retrying. That is deliberate; see the
[error classification table](../reference/error-classification.md).
