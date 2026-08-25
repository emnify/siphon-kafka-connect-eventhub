package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.core.amqp.exception.AmqpErrorCondition;
import com.azure.core.amqp.exception.AmqpErrorContext;
import com.azure.core.amqp.exception.AmqpException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EventHubErrorsTest {

    private static final AmqpErrorContext CONTEXT = new AmqpErrorContext("namespace.servicebus.windows.net");

    private static AmqpException amqp(boolean isTransient, AmqpErrorCondition condition) {
        return new AmqpException(isTransient, condition, "simulated", CONTEXT);
    }

    @Test
    public void throttlingIsRetriable() {
        assertTrue(EventHubErrors.isRetriable(amqp(true, AmqpErrorCondition.SERVER_BUSY_ERROR)));
    }

    @Test
    public void linkDetachIsRetriable() {
        // The single most common intermittent failure: the service reclaims an idle link.
        assertTrue(EventHubErrors.isRetriable(amqp(true, AmqpErrorCondition.LINK_DETACH_FORCED)));
    }

    @Test
    public void connectionForcedIsRetriableEvenWhenTheSdkDoesNotSaySo() {
        assertTrue(EventHubErrors.isRetriable(amqp(false, AmqpErrorCondition.CONNECTION_FORCED)));
    }

    @Test
    public void badCredentialsAreNotRetriable() {
        // Marked transient by the SDK, but retrying a rejected credential just spins until errors.retry.timeout.
        assertFalse(EventHubErrors.isRetriable(amqp(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS)));
    }

    @Test
    public void oversizedPayloadIsNotRetriable() {
        assertFalse(EventHubErrors.isRetriable(amqp(true, AmqpErrorCondition.LINK_PAYLOAD_SIZE_EXCEEDED)));
    }

    @Test
    public void missingEntityIsNotRetriable() {
        assertFalse(EventHubErrors.isRetriable(amqp(false, AmqpErrorCondition.NOT_FOUND)));
    }

    @Test
    public void conditionlessExceptionFallsBackToTheTransientFlag() {
        assertTrue(EventHubErrors.isRetriable(new AmqpException(true, "simulated", CONTEXT)));
        assertFalse(EventHubErrors.isRetriable(new AmqpException(false, "simulated", CONTEXT)));
    }

    @Test
    public void causeChainIsUnwrapped() {
        Throwable wrapped = new ExecutionException(
                new RuntimeException("reactor wrapper", amqp(true, AmqpErrorCondition.SERVER_BUSY_ERROR)));
        assertTrue(EventHubErrors.isRetriable(wrapped));
    }

    @Test
    public void networkFailuresAreRetriable() {
        assertTrue(EventHubErrors.isRetriable(new IOException("connection reset")));
        assertTrue(EventHubErrors.isRetriable(new UnknownHostException("dns")));
        assertTrue(EventHubErrors.isRetriable(new TimeoutException("no ack")));
    }

    @Test
    public void unrecognisedFailuresAreNotRetriable() {
        assertFalse(EventHubErrors.isRetriable(new IllegalArgumentException("bad config")));
    }

    @Test
    public void cyclicCauseChainTerminates() {
        // A self-referencing cause is legal and would hang a naive while(cause != null) walk on the task thread.
        Exception first = new Exception("first");
        Exception second = new Exception("second", first);
        first.initCause(second);
        assertFalse(EventHubErrors.isRetriable(first));
        assertTrue(EventHubErrors.describe(first).contains("second"));
    }

    @Test
    public void retriableFailuresBecomeRetriableExceptions() {
        ConnectException converted = EventHubErrors.toConnectException(
                "boom", amqp(true, AmqpErrorCondition.SERVER_BUSY_ERROR));
        assertTrue(converted instanceof RetriableException);
    }

    @Test
    public void permanentFailuresBecomePlainConnectExceptions() {
        ConnectException converted = EventHubErrors.toConnectException(
                "boom", amqp(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS));
        assertEquals(ConnectException.class, converted.getClass());
    }

    @Test
    public void describeNamesTheAmqpCondition() {
        String described = EventHubErrors.describe(
                new ExecutionException(amqp(true, AmqpErrorCondition.SERVER_BUSY_ERROR)));
        assertTrue(described, described.contains("SERVER_BUSY_ERROR"));
        assertTrue(described, described.contains("transient=true"));
    }
}
