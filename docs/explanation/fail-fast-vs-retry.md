# Fail fast or retry

Two failure modes need opposite treatment, and the whole reliability design of this connector is the line between
them.

| | Configuration fault | Intermittent fault |
|---|---|---|
| Examples | wrong key, missing `Send` claim, misspelled `EntityPath`, disabled Event Hub, oversized event | throttling, link detach, connection recycle, DNS blip, network partition |
| Will the next attempt differ? | no, never | probably |
| Right response | stop immediately and say why | absorb it, keep the pipeline running |
| Exception | `ConnectException` with an `EVENTHUB_*` token | `RetriableException` |
| Operator sees | an actionable message and a failed task | a warning, and throughput |

Both mistakes are expensive, in opposite ways.

**Retrying a configuration fault** wastes the entire `errors.retry.timeout` window — potentially ten minutes of a
dead pipeline — and then fails with exactly the message it already had on the first attempt. Worse, the connector
looks alive while it makes no progress, so monitoring based on task state does not fire.

**Failing on an intermittent fault** kills a healthy pipeline over a link the service reclaimed because it was
idle, and requires a human to restart it. This is the failure this connector was rewritten to stop doing.

## Failing fast at startup

The v5 SDK's `buildProducerClient()` opens no connection — the AMQP link is established lazily, on the first send.
Left alone, a task configured with a rejected credential reports `RUNNING` and only fails when the first record
arrives, which may be hours later and on somebody else's shift.

So `start()` proves the connection before reporting success. The probe is `createBatch()`, which attaches the send
link and therefore exercises authentication against the real entity. It is deliberately *not*
`getEventHubProperties()`: that reads entity metadata and needs the `Listen` claim, which would reject the
least-privilege `Send`-only policy this connector recommends.

The two outcomes are asymmetric on purpose:

- A **configuration fault** fails immediately, with its token and an explanation. `eventhub.start.timeout` does
  not apply — retrying a rejected credential only delays the same failure.
- A **transient fault** is retried with exponential backoff for up to `eventhub.start.timeout` (60s by default),
  so a worker restarting during a brief network blip comes up on its own instead of needing a manual nudge.

Set `eventhub.validate.on.start=false` to skip the probe. The only good reason is an environment where the Event
Hub legitimately does not exist yet at task start.

## Failing fast during `put()`

The same classification runs on send failures. A permanent failure beats a concurrent transient one, so a poison
batch cannot hide behind a throttling response and retry until the timeout expires.

## Where the boundary is drawn

In `EventHubErrors`, and only there. Two overrides of the SDK's own `isTransient()` flag are worth knowing:

- `UNAUTHORIZED_ACCESS` is flagged transient by the SDK. It is not, for us — a rejected credential is rejected
  identically every time.
- `CONNECTION_FORCED` is sometimes flagged non-transient. It is transient — Azure recycles connections during
  deployments and the next attempt succeeds.

The full table is in the [error classification reference](../reference/error-classification.md).
