# How to authenticate with a filesystem JWT

Use this when a sidecar or an init process writes a short-lived JWT to disk and rotates it, rather than the
connector holding a long-lived shared access key.

## Layout on disk

The connector reads the token from:

```
<eventhub.auth.token.dir>/<namespace>/<entityPath>
```

For a connection string of
`Endpoint=sb://ns.servicebus.windows.net/;EntityPath=events` and a token directory of `/var/run/eh-tokens`, that
is:

```
/var/run/eh-tokens/ns.servicebus.windows.net/events
```

The file holds the JWT on a single line. Leading and trailing whitespace and blank lines are ignored.

This is the same layout the previous v3-SDK build used, so an existing sidecar needs no changes.

## Configure

```properties
eventhub.authentication=JWT
eventhub.auth.token.dir=/var/run/eh-tokens
eventhub.connection.string=Endpoint=sb://ns.servicebus.windows.net/;EntityPath=events
```

The connection string is still required — it is where the namespace and Event Hub name come from — but its
`SharedAccessKey` component, if present, is unused.

If `eventhub.auth.token.dir` is not set, the connector falls back to the `CONNECT_AUTH_TOKEN_DIR` environment
variable. Prefer the property: it is visible in the REST API, whereas the environment variable is invisible to
anyone debugging the connector.

## Rotation

The connector caches the token and re-reads the file when the current token is within **5 minutes** of expiring,
so a sidecar that replaces the file in place is picked up without a restart.

Expiry comes from the JWT's `exp` claim. If the token is not a three-part JWT, or has no `exp`, the connector
assumes a 5-minute lifetime and re-reads that often — deliberately short, because a wrong-but-long guess would
keep a dead token in the cache and turn a rotation into an outage.

Write the file **atomically** — write to a temporary name in the same directory and `rename(2)` over the target.
A non-atomic replace can be observed mid-write; the connector treats the resulting `IOException` as retriable, but
a clean rename avoids the noise entirely.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `ConnectException: ... no token directory is configured` | Neither `eventhub.auth.token.dir` nor `CONNECT_AUTH_TOKEN_DIR` is set. |
| `ConnectException: ... the connection string has no EntityPath` | JWT auth needs the Event Hub name to build the token path. |
| `UncheckedIOException: Unable to read Event Hubs JWT from ...` | The file is missing, unreadable, or empty. Retriable — check the sidecar. |
| `WARN Token in ... has no 'exp' claim` | The sidecar is writing something that is not a JWT, or is writing the wrong field. |
| `AmqpException[condition=UNAUTHORIZED_ACCESS]` | The token is valid JSON but rejected by Event Hubs — wrong audience, wrong signer, or already expired. |

To confirm which file the connector is using, look for this at startup:

```
INFO Filesystem JWT credential will read tokens from /var/run/eh-tokens/ns.servicebus.windows.net/events
```
