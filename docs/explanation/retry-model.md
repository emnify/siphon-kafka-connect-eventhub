# The two-layer retry model

The connector retries in two independent places. Understanding which layer handles what is the difference between
a connector that shrugs off a throttling burst and one that pages you at 03:00.

```
                    Kafka Connect
                          |
                    put(records)
                          |
   +----------------------v-----------------------+
   |  layer 2:  EventHubSinkTask                   |
   |  errors.retry.timeout, RetriableException     |
   |  re-delivers the WHOLE batch                  |
   |                                               |
   |   +------------------v--------------------+   |
   |   |  layer 1:  Event Hubs SDK             |   |
   |   |  AmqpRetryOptions                     |   |
   |   |  retries ONE send, invisibly          |   |
   |   +------------------+--------------------+   |
   +----------------------|-----------------------+
                          v
                    Azure Event Hubs
```

## Layer 1 — inside the SDK

`AmqpRetryOptions` retries a single failed send, with backoff, without the connector ever seeing it. This absorbs
the overwhelming majority of intermittent failures: a link the service reclaimed because it was idle, a brief
throttling response, a connection recycled during an Azure deployment.

Configured by `eventhub.client.retry.count`, `.mode`, `.minimumBackoff`, `.maximumBackoff` and
`eventhub.client.try.timeout`.

Layer 1 is cheap and precise — it retries exactly the failed send, keeps the batch in memory, and does not disturb
Kafka Connect. Prefer it. Widening layer 1 is almost always better than relying on layer 2.

## Layer 2 — inside Kafka Connect

When the SDK exhausts its retries the failure reaches `put()`. The task classifies it (see
[error classification](../reference/error-classification.md)) and, if it is transient, raises `RetriableException`.
Connect then re-delivers the *same records* to `put()` after a backoff, for as long as `errors.retry.timeout`
allows.

Layer 2 is coarse: it replays the whole batch, including the batches that already succeeded. That is why the
connector is [at-least-once](delivery-semantics.md). It exists for failures that outlast layer 1 — an Event Hub
that is throttling for minutes, a network partition, a namespace being scaled.

## Why `errors.retry.timeout` matters more than it looks

Its default is `0`. **With the default, layer 2 does not exist.** A transient failure that survives the SDK's
retries fails the task immediately, and the connector stays dead until someone restarts it. Anyone who cares about
intermittent errors must set it.

## The nesting constraint

Three timeouts have to nest, innermost first:

```
(retry.count + 1) x try.timeout  +  retry.count x maximumBackoff     layer 1's worst case
        <  eventhub.send.timeout                                      layer 2's deadline
                <  consumer.max.poll.interval.ms                      Kafka's patience
```

Break the first inequality and layer 2 abandons batches the SDK was still usefully retrying — throttling that
would have cleared on its own becomes an endless retry loop that never makes progress.

Break the second and something worse happens: the task blocks past `max.poll.interval.ms`, the consumer is
evicted from its group, the group rebalances, the partitions move, and the new owner replays from the last
committed offset. A slow Event Hub has turned into a rebalance storm. This failure mode is self-sustaining —
the rebalance makes the next `put()` slower, which triggers the next rebalance.

The shipped defaults satisfy both (210s < 240s < 300s), `EventHubSinkConfig` documents the arithmetic, and
`EventHubSinkTask` logs a warning at startup if a custom configuration inverts the first inequality.

## What is deliberately not retried

Retrying a permanent failure is not free — it costs the whole `errors.retry.timeout` window before the task dies
anyway, during which the connector makes no progress and emits no useful signal. Bad credentials, a missing Event
Hub, and an oversized event are all classified as permanent for that reason, even where the SDK marks them
transient.

The mirror-image mistake is worse: a transient failure classified as permanent kills a healthy pipeline over a
link detach that would have healed by itself.
