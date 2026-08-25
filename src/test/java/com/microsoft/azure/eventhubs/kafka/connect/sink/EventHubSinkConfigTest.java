package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.core.amqp.AmqpRetryMode;
import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.core.amqp.AmqpTransportType;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class EventHubSinkConfigTest {

    private static final String CONNECTION_STRING =
            "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=k;EntityPath=hub";

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(EventHubSinkConfig.CONNECTION_STRING, CONNECTION_STRING);
        return props;
    }

    @Test
    public void jwtAuthenticationIsDetectedByValue() {
        // Regression: the previous implementation compared the configured value with == against a constant. The
        // value arrives from a parsed map and is not interned, so JWT auth silently fell back to SAS.
        Map<String, String> props = baseProps();
        props.put(EventHubSinkConfig.AUTHENTICATION_PROVIDER, new String(new char[]{'J', 'W', 'T'}));
        assertTrue(new EventHubSinkConfig(props).isJwtAuthentication());
    }

    @Test
    public void sasIsTheDefaultAuthentication() {
        assertFalse(new EventHubSinkConfig(baseProps()).isJwtAuthentication());
    }

    @Test
    public void connectionStringIsMaskedInToString() {
        // Type.PASSWORD keeps the key out of the Connect REST API and out of AbstractConfig's own logging.
        EventHubSinkConfig config = new EventHubSinkConfig(baseProps());
        assertEquals(CONNECTION_STRING, config.connectionString());
        assertEquals(-1, config.getPassword(EventHubSinkConfig.CONNECTION_STRING).toString().indexOf("SharedAccessKey=k"));
    }

    @Test
    public void retryOptionsAreBuiltFromTheConfiguredValues() {
        Map<String, String> props = baseProps();
        props.put(EventHubSinkConfig.CLIENT_RETRY_COUNT, "7");
        props.put(EventHubSinkConfig.CLIENT_RETRY_MIN_BACKOFF, "2");
        props.put(EventHubSinkConfig.CLIENT_RETRY_MAX_BACKOFF, "45");
        props.put(EventHubSinkConfig.CLIENT_RETRY_MODE, "FIXED");
        props.put(EventHubSinkConfig.CLIENT_TRY_TIMEOUT, "20");

        AmqpRetryOptions options = new EventHubSinkConfig(props).retryOptions();
        assertEquals(7, options.getMaxRetries());
        assertEquals(Duration.ofSeconds(2), options.getDelay());
        assertEquals(Duration.ofSeconds(45), options.getMaxDelay());
        assertEquals(Duration.ofSeconds(20), options.getTryTimeout());
        assertEquals(AmqpRetryMode.FIXED, options.getMode());
    }

    @Test
    public void maxBackoffBelowMinBackoffIsClamped() {
        // AmqpRetryOptions rejects maxDelay < delay outright; clamping keeps a sloppy config from failing the task.
        Map<String, String> props = baseProps();
        props.put(EventHubSinkConfig.CLIENT_RETRY_MIN_BACKOFF, "30");
        props.put(EventHubSinkConfig.CLIENT_RETRY_MAX_BACKOFF, "5");
        AmqpRetryOptions options = new EventHubSinkConfig(props).retryOptions();
        assertEquals(Duration.ofSeconds(30), options.getDelay());
        assertEquals(Duration.ofSeconds(30), options.getMaxDelay());
    }

    @Test
    public void defaultsAreProductionSafe() {
        EventHubSinkConfig config = new EventHubSinkConfig(baseProps());
        assertEquals(1, config.clientsPerTask());
        assertEquals(AmqpTransportType.AMQP, config.transportType());
        assertFalse(config.isPartitionKeyFromRecordKey());
        // The put() deadline must stay under the default max.poll.interval.ms of 300s, or a slow Event Hub turns
        // into a consumer group rebalance.
        assertTrue(config.sendTimeout().getSeconds() < 300);
    }

    @Test
    public void defaultTimeoutsNest() {
        // client retry budget < eventhub.send.timeout < Kafka's default max.poll.interval.ms.
        // Invert any of these and an intermittent failure becomes a rebalance loop instead of a retry.
        EventHubSinkConfig config = new EventHubSinkConfig(baseProps());
        AmqpRetryOptions retry = config.retryOptions();
        Duration worstCase = retry.getTryTimeout()
                .multipliedBy(retry.getMaxRetries() + 1L)
                .plus(retry.getMaxDelay().multipliedBy(retry.getMaxRetries()));

        assertTrue("client retry budget " + worstCase + " must fit inside " + config.sendTimeout(),
                worstCase.compareTo(config.sendTimeout()) < 0);
        assertTrue(config.sendTimeout().compareTo(Duration.ofSeconds(300)) < 0);
    }

    @Test
    public void recordKeyPartitioningCanBeEnabled() {
        Map<String, String> props = baseProps();
        props.put(EventHubSinkConfig.PARTITION_KEY_SOURCE, EventHubSinkConfig.PARTITION_KEY_RECORD_KEY);
        assertTrue(new EventHubSinkConfig(props).isPartitionKeyFromRecordKey());
    }

    @Test
    public void invalidEnumeratedValuesAreRejectedAtCreateTime() {
        Map<String, String> auth = baseProps();
        auth.put(EventHubSinkConfig.AUTHENTICATION_PROVIDER, "OAUTH");
        assertThrows(ConfigException.class, () -> new EventHubSinkConfig(auth));

        Map<String, String> transport = baseProps();
        transport.put(EventHubSinkConfig.TRANSPORT_TYPE, "HTTP");
        assertThrows(ConfigException.class, () -> new EventHubSinkConfig(transport));

        Map<String, String> keySource = baseProps();
        keySource.put(EventHubSinkConfig.PARTITION_KEY_SOURCE, "record.value");
        assertThrows(ConfigException.class, () -> new EventHubSinkConfig(keySource));
    }

    @Test
    public void nonPositiveClientCountIsRejected() {
        Map<String, String> props = baseProps();
        props.put(EventHubSinkConfig.CLIENTS_PER_TASK, "0");
        assertThrows(ConfigException.class, () -> new EventHubSinkConfig(props));
    }

    @Test
    public void missingConnectionStringIsRejected() {
        assertThrows(ConfigException.class, () -> new EventHubSinkConfig(new HashMap<>()));
    }

    @Test
    public void authTokenDirPropertyWins() {
        Map<String, String> props = baseProps();
        props.put(EventHubSinkConfig.AUTH_TOKEN_DIR, "/var/run/eventhub-tokens");
        assertEquals("/var/run/eventhub-tokens", new EventHubSinkConfig(props).authTokenDir());
    }
}
