package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.core.amqp.exception.AmqpErrorCondition;
import com.azure.core.amqp.exception.AmqpErrorContext;
import com.azure.core.amqp.exception.AmqpException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Locks the shape of the failures this connector reports, because a downstream service parses them out of the
 * stack trace by regular expression.
 *
 * <p>Kafka Connect records a failed task's stack trace verbatim in {@code GET /connectors/<name>/status}.
 * ecds-api recovers the customer-facing message from that string in {@code src/client/connect.ts}:
 *
 * <pre>
 * _parseException(traces) {
 *   return traces.map(v =&gt; v?.match(
 *     /\s*Caused by: (org\.apache\.kafka\.connect\.errors\.ConnectException|kafka\.connect\.http\.sink\.errors.+):(.*)/
 *   )?.[2]).filter(f =&gt; f !== null &amp;&amp; f !== undefined)
 * }
 * </pre>
 *
 * and, in {@code src/api/requests.ts}, shows the customer {@code 'Error state: ' + traces.join(',')} - or
 * {@code 'Error state: Unknown error'} when nothing matched.
 *
 * <p>Three properties therefore have to hold, and none of them is obvious from reading this connector alone:
 *
 * <ol>
 *   <li>the reported exception's class must be <em>exactly</em> {@code ConnectException}, not a subclass;</li>
 *   <li>the trace must contain a {@code Caused by:} line for it, which Connect supplies for {@code put()}
 *       failures but <em>not</em> for {@code start()} failures;</li>
 *   <li>the message must be a single line, because the capture group is {@code (.*)}.</li>
 * </ol>
 *
 * <p>These tests fail loudly if a future change breaks any of them, rather than silently degrading every
 * misconfiguration to "Unknown error".
 */
public class CustomerFacingErrorContractTest {

    /** Verbatim copy of the pattern in ecds-api {@code src/client/connect.ts}. */
    private static final Pattern ECDS_API_TRACE_PATTERN = Pattern.compile(
            "\\s*Caused by: (org\\.apache\\.kafka\\.connect\\.errors\\.ConnectException"
                    + "|kafka\\.connect\\.http\\.sink\\.errors.+):(.*)");

    private static final AmqpErrorContext AMQP_CONTEXT = new AmqpErrorContext("ns.servicebus.windows.net");

    /**
     * What the customer would see, or {@code null} if ecds-api would fall back to "Unknown error".
     */
    private static String customerFacingMessage(Throwable recordedByConnect) {
        StringWriter trace = new StringWriter();
        recordedByConnect.printStackTrace(new PrintWriter(trace));
        Matcher matcher = ECDS_API_TRACE_PATTERN.matcher(trace.toString());
        return matcher.find() ? matcher.group(2).trim() : null;
    }

    /** How {@code WorkerSinkTask.deliverMessages} wraps an exception thrown by {@code put()}. */
    private static Throwable asRecordedForPutFailure(Throwable thrownByPut) {
        return new ConnectException("Exiting WorkerSinkTask due to unrecoverable exception.", thrownByPut);
    }

    /** {@code WorkerTask.doRun} records a {@code start()} failure with no wrapping of its own. */
    private static Throwable asRecordedForStartFailure(Throwable thrownByStart) {
        return thrownByStart;
    }

    @Test
    public void aRejectedCredentialFromPutReachesTheCustomer() {
        AmqpException cause =
                new AmqpException(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS, "Unauthorized", AMQP_CONTEXT);
        ConnectException thrown = new ConnectException(EventHubErrors.diagnose(cause), cause);

        String shown = customerFacingMessage(asRecordedForPutFailure(thrown));

        assertTrue("ecds-api would show 'Unknown error'", shown != null);
        assertTrue(shown, shown.startsWith("[" + EventHubErrors.TOKEN_UNAUTHORIZED + "]"));
        assertTrue(shown, shown.contains("'Send' claim"));
    }

    @Test
    public void aRejectedCredentialFromStartReachesTheCustomer() {
        // The regression this test exists for: Connect does not wrap start() failures, so an unwrapped
        // ConnectException has no "Caused by:" line and the customer is told nothing at all.
        AmqpException cause =
                new AmqpException(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS, "Unauthorized", AMQP_CONTEXT);
        Throwable thrown = startupFailureAsTheTaskWouldThrowIt(new ConnectException(EventHubErrors.diagnose(cause), cause));

        String shown = customerFacingMessage(asRecordedForStartFailure(thrown));

        assertTrue("ecds-api would show 'Unknown error'", shown != null);
        assertTrue(shown, shown.contains(EventHubErrors.TOKEN_UNAUTHORIZED));
    }

    @Test
    public void anUnwrappedStartFailureWouldNotReachTheCustomer() {
        // Demonstrates why the wrapping above is necessary, so nobody removes it as redundant.
        AmqpException cause =
                new AmqpException(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS, "Unauthorized", AMQP_CONTEXT);
        ConnectException unwrapped = new ConnectException(EventHubErrors.diagnose(cause), cause);

        assertEquals(null, customerFacingMessage(asRecordedForStartFailure(unwrapped)));
    }

    @Test
    public void aConnectExceptionSubclassWouldNotReachTheCustomer() {
        // Documents why EventHubErrors.toConnectException must return exactly ConnectException.
        class NicerLookingException extends ConnectException {
            private static final long serialVersionUID = 1L;

            NicerLookingException(String message) {
                super(message);
            }
        }

        assertEquals(null, customerFacingMessage(
                asRecordedForPutFailure(new NicerLookingException("[EVENTHUB_UNAUTHORIZED] would be lost"))));
    }

    @Test
    public void everyDiagnosisIsASingleLine() {
        // The capture group is (.*), which stops at the first newline.
        for (AmqpErrorCondition condition : AmqpErrorCondition.values()) {
            String diagnosis = EventHubErrors.diagnose(
                    new AmqpException(false, condition, "simulated", AMQP_CONTEXT));
            if (diagnosis != null) {
                assertEquals(diagnosis, -1, diagnosis.indexOf('\n'));
                assertEquals(diagnosis, -1, diagnosis.indexOf('\r'));
                assertTrue(diagnosis, diagnosis.startsWith("[EVENTHUB_"));
            }
        }
    }

    @Test
    public void everyDiagnosedConditionIsAlsoTreatedAsPermanent() {
        // A diagnosed configuration fault that was still classified retriable would be retried for the whole
        // errors.retry.timeout window before failing with the message it already had at the first attempt.
        for (AmqpErrorCondition condition : AmqpErrorCondition.values()) {
            AmqpException ex = new AmqpException(true, condition, "simulated", AMQP_CONTEXT);
            if (EventHubErrors.diagnose(ex) != null) {
                assertTrue(condition.name(), !EventHubErrors.isRetriable(ex));
            }
        }
    }

    @Test
    public void aTransientFailureCarriesNoDiagnosis() {
        // Throttling is not a configuration fault and must not be reported to the customer as one.
        assertEquals(null, EventHubErrors.diagnose(
                new AmqpException(true, AmqpErrorCondition.SERVER_BUSY_ERROR, "busy", AMQP_CONTEXT)));
        assertEquals(null, EventHubErrors.diagnose(
                new AmqpException(true, AmqpErrorCondition.LINK_DETACH_FORCED, "detached", AMQP_CONTEXT)));
    }

    @Test
    public void noDiagnosisEverLeaksACredential() {
        String secret = "SharedAccessKey=AbCdEf0123456789+/=";
        for (AmqpErrorCondition condition : AmqpErrorCondition.values()) {
            String diagnosis = EventHubErrors.diagnose(
                    new AmqpException(false, condition, "failed while using " + secret, AMQP_CONTEXT));
            if (diagnosis != null) {
                assertEquals(diagnosis, -1, diagnosis.indexOf("AbCdEf0123456789"));
            }
        }
    }

    /** Mirrors {@code EventHubSinkTask.startupFailure}. */
    private static Throwable startupFailureAsTheTaskWouldThrowIt(ConnectException reportable) {
        return new ConnectException("EventHubSinkTask failed to start. " + reportable.getMessage(), reportable);
    }
}
