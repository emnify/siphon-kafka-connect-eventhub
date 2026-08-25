package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.core.amqp.exception.AmqpErrorCondition;
import com.azure.core.amqp.exception.AmqpException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Classifies Event Hubs failures into "retry the batch" and "fail the task".
 *
 * <p>There are two independent layers of retry, and both matter:
 *
 * <ol>
 *   <li><b>Inside the SDK.</b> {@code AmqpRetryOptions} retries a transient send a bounded number of times with
 *       backoff, without the connector noticing. This absorbs the common case: a link detach, a throttling
 *       response, a brief connection blip.</li>
 *   <li><b>Inside Kafka Connect.</b> When the SDK gives up, the failure reaches {@code put()}. Throwing
 *       {@link RetriableException} tells the framework to re-deliver the same records after a backoff, up to
 *       {@code errors.retry.timeout}. Throwing anything else kills the task and requires an operator restart.</li>
 * </ol>
 *
 * <p>The rule this class encodes: a failure that a later attempt could plausibly succeed at is retriable; a
 * failure that will fail identically forever is not. Getting this wrong in either direction is costly - a
 * permanent error marked retriable spins forever, and a transient error marked permanent pages an operator at
 * 03:00 for a link detach that would have healed on its own.
 *
 * <p>Retrying re-sends records the previous attempt may already have delivered, so the connector is at-least-once
 * and downstream consumers must tolerate duplicates.
 */
final class EventHubErrors {

    /**
     * AMQP conditions that are permanent regardless of what {@link AmqpException#isTransient()} reports.
     * Retrying any of these just burns the retry budget before failing anyway.
     */
    private static final Set<AmqpErrorCondition> PERMANENT_CONDITIONS = EnumSet.of(
            AmqpErrorCondition.UNAUTHORIZED_ACCESS,        // bad or expired credentials, missing Send claim
            AmqpErrorCondition.NOT_FOUND,                  // namespace or Event Hub does not exist
            AmqpErrorCondition.ENTITY_DISABLED_ERROR,      // Event Hub disabled in the portal
            AmqpErrorCondition.NOT_ALLOWED,                // operation not permitted on this entity
            AmqpErrorCondition.NOT_IMPLEMENTED,
            AmqpErrorCondition.ARGUMENT_ERROR,
            AmqpErrorCondition.ARGUMENT_OUT_OF_RANGE_ERROR,
            AmqpErrorCondition.LINK_PAYLOAD_SIZE_EXCEEDED, // the event is larger than the Event Hub allows
            AmqpErrorCondition.PUBLISHER_REVOKED_ERROR);

    /**
     * AMQP conditions that are transient even when the SDK does not flag them as such. These are the intermittent
     * failures this connector exists to survive.
     */
    private static final Set<AmqpErrorCondition> TRANSIENT_CONDITIONS = EnumSet.of(
            AmqpErrorCondition.SERVER_BUSY_ERROR,          // Event Hubs throttling; back off and retry
            AmqpErrorCondition.TIMEOUT_ERROR,
            AmqpErrorCondition.INTERNAL_ERROR,
            AmqpErrorCondition.LINK_DETACH_FORCED,         // idle link reclaimed by the service
            AmqpErrorCondition.CONNECTION_FORCED,          // service-side connection recycle, e.g. during a deploy
            AmqpErrorCondition.LINK_STOLEN,
            AmqpErrorCondition.CONNECTION_FRAMING_ERROR,
            AmqpErrorCondition.PROTON_IO,
            AmqpErrorCondition.OPERATION_CANCELLED,
            AmqpErrorCondition.RESOURCE_LIMIT_EXCEEDED,    // concurrent-connection cap; frees up on its own
            AmqpErrorCondition.TRANSFER_LIMIT_EXCEEDED);   // link credit exhausted

    private EventHubErrors() {
    }

    /**
     * Whether re-sending the same records later could plausibly succeed.
     *
     * <p>Unwraps the cause chain, because a transient AMQP failure routinely arrives wrapped in an
     * {@code ExecutionException}, a {@code CompletionException}, or a reactor {@code RuntimeException}.
     */
    static boolean isRetriable(Throwable throwable) {
        for (Throwable cause : causeChain(throwable)) {
            if (cause instanceof AmqpException) {
                AmqpException amqp = (AmqpException) cause;
                AmqpErrorCondition condition = amqp.getErrorCondition();
                if (condition != null && PERMANENT_CONDITIONS.contains(condition)) {
                    return false;
                }
                if (condition != null && TRANSIENT_CONDITIONS.contains(condition)) {
                    return true;
                }
                return amqp.isTransient();
            }
            if (cause instanceof TimeoutException || cause instanceof SocketTimeoutException) {
                return true;
            }
            if (cause instanceof UnknownHostException) {
                // DNS. Almost always a resolver hiccup or a node that just lost its network namespace, not a typo
                // that survived a working start(), so treat it as worth another attempt.
                return true;
            }
            if (cause instanceof SocketException || cause instanceof ClosedChannelException) {
                return true;
            }
            if (cause instanceof IOException) {
                return true;
            }
            if (cause instanceof IllegalStateException && isClosedClient(cause)) {
                // The producer was recycled underneath us; start()/put() will rebuild it on the next attempt.
                return true;
            }
            if (cause instanceof RejectedExecutionException) {
                return false;
            }
        }
        return false;
    }

    /**
     * Wraps a failure in the exception type that produces the behaviour we want from the Connect framework.
     *
     * <p><b>The permanent case must be exactly {@link ConnectException}, never a subclass.</b> Kafka Connect
     * records a failed task's stack trace verbatim, and ecds-api recovers the customer-facing message from it with
     *
     * <pre>/\s*Caused by: (org\.apache\.kafka\.connect\.errors\.ConnectException|...):(.*)/</pre>
     *
     * That pattern matches the class name literally. A subclass - however much better it would read in the code -
     * prints its own fully qualified name, fails the match, and the customer is shown "Error state: Unknown error"
     * instead of the explanation this class works so hard to produce. The failure kind is carried in the message
     * as a leading token instead; see {@link #diagnose}.
     *
     * @param message operator-facing context, on a single line; must not contain the connection string or a token
     */
    static ConnectException toConnectException(String message, Throwable cause) {
        if (isRetriable(cause)) {
            return new RetriableException(message, cause);
        }
        return new ConnectException(message, cause);
    }

    /**
     * Stable, machine-readable token prefixed to every operator-facing failure message.
     *
     * <p>Mirrors the {@code errorToken} convention in ecds-api's {@code src/common/errors.ts}: a token a caller can
     * branch on, followed by prose a human can act on. The token lives inside the message rather than in the
     * exception type - see {@link #toConnectException} for why the type cannot vary.
     */
    static final String TOKEN_UNAUTHORIZED = "EVENTHUB_UNAUTHORIZED";
    static final String TOKEN_NOT_FOUND = "EVENTHUB_NOT_FOUND";
    static final String TOKEN_DISABLED = "EVENTHUB_DISABLED";
    static final String TOKEN_NOT_ALLOWED = "EVENTHUB_NOT_ALLOWED";
    static final String TOKEN_PAYLOAD_TOO_LARGE = "EVENTHUB_PAYLOAD_TOO_LARGE";
    static final String TOKEN_PUBLISHER_REVOKED = "EVENTHUB_PUBLISHER_REVOKED";
    static final String TOKEN_BAD_REQUEST = "EVENTHUB_BAD_REQUEST";

    /**
     * Explains a permanent Event Hubs failure in terms of what an operator should change.
     *
     * <p>The SDK's own message names the AMQP condition and nothing else, which is not enough to act on at 03:00.
     *
     * <p>The result is always a single line. Downstream, ecds-api extracts the customer-facing message with a
     * regular expression whose trailing group is {@code (.*)}, which stops at the first newline - a multi-line
     * message would be silently truncated.
     *
     * @return "[TOKEN] explanation", or {@code null} if this failure is not a recognised configuration fault
     */
    static String diagnose(Throwable throwable) {
        for (Throwable cause : causeChain(throwable)) {
            if (!(cause instanceof AmqpException)) {
                continue;
            }
            AmqpErrorCondition condition = ((AmqpException) cause).getErrorCondition();
            if (condition == null) {
                return null;
            }
            switch (condition) {
                case UNAUTHORIZED_ACCESS:
                    return "[" + TOKEN_UNAUTHORIZED + "] Event Hubs rejected the credential. Check that "
                            + EventHubSinkConfig.CONNECTION_STRING + " carries the right SharedAccessKey and that the "
                            + "policy grants the 'Send' claim on this Event Hub. With "
                            + EventHubSinkConfig.AUTHENTICATION_PROVIDER + "="
                            + EventHubSinkConfig.JWT_AUTHENTICATION_PROVIDER + ", check that the token file holds a "
                            + "current, correctly scoped JWT.";
                case NOT_FOUND:
                    return "[" + TOKEN_NOT_FOUND + "] Event Hubs reports that the namespace or the Event Hub does not "
                            + "exist. Check the Endpoint host and the EntityPath component of "
                            + EventHubSinkConfig.CONNECTION_STRING + " for a typo.";
                case ENTITY_DISABLED_ERROR:
                    return "[" + TOKEN_DISABLED + "] The target Event Hub is disabled. Re-enable it in the Azure "
                            + "portal.";
                case NOT_ALLOWED:
                    return "[" + TOKEN_NOT_ALLOWED + "] Event Hubs does not allow this operation on the target "
                            + "entity. Check that " + EventHubSinkConfig.CONNECTION_STRING + " points at an Event Hub "
                            + "rather than at a consumer group or a Service Bus queue.";
                case LINK_PAYLOAD_SIZE_EXCEEDED:
                    return "[" + TOKEN_PAYLOAD_TOO_LARGE + "] The event exceeds the Event Hub's maximum message size. "
                            + "Reduce the record size, or move to a tier with a larger limit.";
                case PUBLISHER_REVOKED_ERROR:
                    return "[" + TOKEN_PUBLISHER_REVOKED + "] This publisher has been revoked on the target Event Hub.";
                case ARGUMENT_ERROR:
                case ARGUMENT_OUT_OF_RANGE_ERROR:
                    return "[" + TOKEN_BAD_REQUEST + "] Event Hubs rejected the request as malformed. Check "
                            + EventHubSinkConfig.CONNECTION_STRING + " and "
                            + EventHubSinkConfig.PARTITION_KEY_SOURCE + ".";
                default:
                    return null;
            }
        }
        return null;
    }

    /** A short, log-safe description of a failure, including the AMQP condition when there is one. */
    static String describe(Throwable throwable) {
        List<Throwable> chain = causeChain(throwable);
        for (Throwable cause : chain) {
            if (cause instanceof AmqpException) {
                AmqpException amqp = (AmqpException) cause;
                return "AmqpException[condition=" + amqp.getErrorCondition()
                        + ", transient=" + amqp.isTransient() + "]: " + amqp.getMessage();
            }
        }
        Throwable root = chain.get(chain.size() - 1);
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    private static boolean isClosedClient(Throwable cause) {
        String message = cause.getMessage();
        return message != null && message.toLowerCase(Locale.ROOT).contains("closed");
    }

    /**
     * The throwable and its causes, outermost first.
     *
     * <p>Walked with an identity set rather than {@code while (cause != null)}: a cause chain that loops back on
     * itself is rare but legal, and the naive walk hangs the task thread on it.
     */
    private static List<Throwable> causeChain(Throwable throwable) {
        List<Throwable> chain = new ArrayList<>();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = throwable; cause != null && seen.add(cause); cause = cause.getCause()) {
            chain.add(cause);
        }
        return chain;
    }
}
