# Error classification

Every failure reaching `put()` is sorted into one of three outcomes. The sorting logic lives in
`EventHubErrors` and is covered by `EventHubErrorsTest`.

| Outcome | Exception raised | What Connect does |
|---|---|---|
| **Retriable** | `org.apache.kafka.connect.errors.RetriableException` | Re-delivers the same records after a backoff, up to `errors.retry.timeout`. |
| **Permanent** | `org.apache.kafka.connect.errors.ConnectException` | Fails the task. An operator must restart it. |
| **Poison record** | `org.apache.kafka.connect.errors.DataException` | Routed to the dead letter queue if one is configured; otherwise fails the task. |

## Error tokens

Every operator-facing message begins with a stable, machine-readable token, following the `errorToken` convention
in ecds-api's `src/common/errors.ts`. Branch on the token, not on the prose.

| Token | Meaning | Fail fast? |
|---|---|---|
| `EVENTHUB_CONFIG` | The connector configuration is invalid before Event Hubs was even contacted — a malformed connection string, an unknown enum value. | yes |
| `EVENTHUB_UNAUTHORIZED` | Event Hubs rejected the credential, or the policy lacks the `Send` claim. | yes |
| `EVENTHUB_NOT_FOUND` | The namespace or Event Hub does not exist. | yes |
| `EVENTHUB_DISABLED` | The Event Hub is disabled in the portal. | yes |
| `EVENTHUB_NOT_ALLOWED` | The target entity does not accept this operation. | yes |
| `EVENTHUB_PAYLOAD_TOO_LARGE` | An event exceeds the Event Hub's maximum message size. | yes |
| `EVENTHUB_PUBLISHER_REVOKED` | The publisher was revoked. | yes |
| `EVENTHUB_BAD_REQUEST` | Event Hubs rejected the request as malformed. | yes |
| `EVENTHUB_UNREACHABLE` | Event Hubs could not be reached, and the failure is not transient. | yes |

Transient failures carry no token — they are not the operator's fault and are retried rather than reported.

## How the message reaches a customer

Kafka Connect records a failed task's stack trace verbatim in `GET /connectors/<name>/status`. ecds-api extracts
the customer-facing text from it in `src/client/connect.ts` with:

```js
/\s*Caused by: (org\.apache\.kafka\.connect\.errors\.ConnectException|kafka\.connect\.http\.sink\.errors.+):(.*)/
```

Three consequences constrain this connector, and `CustomerFacingErrorContractTest` locks all three:

1. **The exception class must be exactly `org.apache.kafka.connect.errors.ConnectException`.** A subclass prints
   its own name, fails the match, and the customer is shown `Error state: Unknown error`. This is why the failure
   kind is carried as a token in the message rather than as an exception type.
2. **There must be a `Caused by:` line.** Connect supplies one for `put()` failures by wrapping them, but records
   `start()` failures verbatim — so the task wraps its own startup failures to restore that shape.
3. **The message must be a single line.** The capture group `(.*)` stops at the first newline.

## AMQP conditions

`AmqpException.isTransient()` is the SDK's own opinion. This connector overrides it in both directions where the
SDK is wrong for a sink connector's purposes.

### Always retriable

| `AmqpErrorCondition` | Typical cause |
|---|---|
| `SERVER_BUSY_ERROR` | Event Hubs throttling — you exceeded the namespace's throughput units. |
| `TIMEOUT_ERROR` | No acknowledgement within the try timeout. |
| `INTERNAL_ERROR` | Service-side fault. |
| `LINK_DETACH_FORCED` | The service reclaimed an idle link. The single most common intermittent failure. |
| `CONNECTION_FORCED` | Service-side connection recycle, e.g. during an Azure deployment. |
| `LINK_STOLEN` | Another client took over the link. |
| `CONNECTION_FRAMING_ERROR`, `PROTON_IO` | Network-level AMQP fault. |
| `OPERATION_CANCELLED` | In-flight operation aborted by the service. |
| `RESOURCE_LIMIT_EXCEEDED` | Namespace concurrent-connection cap reached. |
| `TRANSFER_LIMIT_EXCEEDED` | Link credit exhausted. |

### Never retriable

| `AmqpErrorCondition` | Typical cause |
|---|---|
| `UNAUTHORIZED_ACCESS` | Wrong or expired credential, or the SAS policy lacks the `Send` claim. Marked *transient* by the SDK, which is wrong for us — retrying a rejected credential just burns the retry budget. |
| `NOT_FOUND` | The namespace or Event Hub does not exist. Usually a typo in `EntityPath`. |
| `ENTITY_DISABLED_ERROR` | The Event Hub is disabled in the portal. |
| `NOT_ALLOWED`, `NOT_IMPLEMENTED` | Operation not permitted on this entity. |
| `ARGUMENT_ERROR`, `ARGUMENT_OUT_OF_RANGE_ERROR` | Malformed request. |
| `LINK_PAYLOAD_SIZE_EXCEEDED` | The event is larger than the Event Hub's maximum message size. |
| `PUBLISHER_REVOKED_ERROR` | The publisher was revoked. |

Any other `AmqpErrorCondition` falls back to `AmqpException.isTransient()`.

## Non-AMQP failures

| Throwable | Outcome | Why |
|---|---|---|
| `java.util.concurrent.TimeoutException`, `SocketTimeoutException` | retriable | |
| `UnknownHostException` | retriable | A resolver hiccup, or a node that briefly lost its network namespace. A genuine typo would not have survived a working `start()`. |
| `SocketException`, `ClosedChannelException` | retriable | |
| any other `IOException` | retriable | Covers a token file being replaced non-atomically by a sidecar. |
| `IllegalStateException` mentioning "closed" | retriable | The producer was recycled underneath the task. |
| `RejectedExecutionException` | permanent | The task is shutting down. |
| anything else | permanent | Fail loudly rather than retry blindly. |

The cause chain is unwrapped, so a transient AMQP fault still classifies correctly when it arrives inside an
`ExecutionException`, a `CompletionException` or a reactor wrapper. The walk uses an identity set, because a
self-referencing cause chain is legal and would otherwise hang the task thread.

## Timeouts and interrupts

| Situation | Outcome |
|---|---|
| `eventhub.send.timeout` elapses with batches unacknowledged | retriable — the message names how many of how many batches were outstanding |
| Task thread interrupted mid-send | retriable, and the interrupt flag is restored |
