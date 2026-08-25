# How to connect through a firewall or proxy

Event Hubs AMQP uses TCP port **5671** by default. Many corporate networks only allow 443.

## Use AMQP over WebSockets

```properties
eventhub.transport.type=AMQP_WEB_SOCKETS
```

This tunnels AMQP over TLS on port 443. It costs some throughput to the extra framing, so only use it when plain
AMQP is blocked.

## Ports to open

| Transport | Destination | Port |
|---|---|---|
| `AMQP` | `<namespace>.servicebus.windows.net` | 5671/tcp |
| `AMQP_WEB_SOCKETS` | `<namespace>.servicebus.windows.net` | 443/tcp |

Both need outbound DNS.

## HTTP proxies

The connector does not expose a proxy setting. The Azure SDK reads the standard JVM system properties, so set them
on the Connect worker:

```bash
export KAFKA_OPTS="-Dhttps.proxyHost=proxy.internal -Dhttps.proxyPort=3128 -Dhttp.nonProxyHosts='localhost|127.0.0.1'"
```

Proxies only apply to `AMQP_WEB_SOCKETS`. Plain AMQP on 5671 is not HTTP and will not traverse an HTTP proxy.

Exposing `ProxyOptions` as a connector property is on the [improvement backlog](../../IMPROVEMENTS.md).

## Diagnosing

A blocked port looks like a hang followed by a timeout, not a refusal:

```
WARN Transient failure sending to Event Hubs ...
     AmqpException[condition=TIMEOUT_ERROR, transient=true]
```

Confirm reachability from the worker host itself, not from your laptop:

```bash
timeout 5 bash -c 'cat < /dev/null > /dev/tcp/<namespace>.servicebus.windows.net/5671' \
  && echo open || echo blocked
```
