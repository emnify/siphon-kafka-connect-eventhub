package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.time.Duration;
import java.util.Map;

public class EventHubSinkConfig extends AbstractConfig {
    /**
     * The connection string for the Event Hub. This can be retrieved from the
     * Azure Portal -> Event Hubs -> Your Event Hub -> Overview -> Connection Strings
     */
    public static final String CONNECTION_STRING = "eventhub.connection.string";

    /**
     * The number of Azure EventHubClient objects to use per task.
     * Each client will create its own TCP connection, which helps with gaining more throughput
     */
    public static final String CLIENTS_PER_TASK = "eventhub.clients.per.task";

    /**
     * EventHub serialization type for structured events with schema. By default all structured
     * events are converted to JSON.
     */
    public static final String SERIALIZATION_TYPE = "eventhub.serialization";

    /**
     * EventHub client retry policy - retry count.
     */
    public static final String CLIENT_RETRY_COUNT = "eventhub.client.retry.count";

    /**
     * EventHub client retry policy - minimum backoff.
     */
    public static final String CLIENT_RETRY_MIN_BACKOFF = "eventhub.client.retry.minimumBackoff";

    /**
     * EventHub client retry policy - maximum backoff.
     */
    public static final String CLIENT_RETRY_MAX_BACKOFF = "eventhub.client.retry.maximumBackoff";


    /**
     * EventHub authentication provider. Valid values are SAS or JWT from filesystem.
     */
    public static final String AUTHENTICATION_PROVIDER = "eventhub.authentication";

    /**
     * EventHub SAS (default) authentication provider = "SAS";
     */
    public static final String SAS_AUTHENTICATION_PROVIDER = "SAS";

    /**
     * EventHub SAS (default) authentication provider = "JWT";
     */
    public static final String JWT_AUTHENTICATION_PROVIDER = "JWT";

    /**
     * Send all structured events to EventHub as JSON
     */
    public static final String SERIALIZATION_JSON = "json";

    private static final short defaultClientsPerTask = 1;

    private static final int defaultMaxRetries = 3;

    private static final long defaultMinimumBackoffSeconds = 1;

    private static final long defaultMaximumBackoffSeconds = 30;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONNECTION_STRING, Type.STRING, Importance.HIGH,
                    "EventHub Connection String")
            .define(CLIENTS_PER_TASK, Type.SHORT, defaultClientsPerTask, Importance.HIGH,
                    "Number of Event Hub clients to use per task")
            .define(SERIALIZATION_TYPE, Type.STRING, SERIALIZATION_JSON, Importance.LOW,
                    "Method of serialization of structured events with schema to EventHub")
            .define(AUTHENTICATION_PROVIDER, Type.STRING, SAS_AUTHENTICATION_PROVIDER, Importance.HIGH,
                    "Method of EventHub authentication")
            .define(CLIENT_RETRY_COUNT, Type.INT, defaultMaxRetries, Importance.LOW,
                    "Maximum number of retries for EventHub client")
            .define(CLIENT_RETRY_MIN_BACKOFF, Type.LONG, defaultMinimumBackoffSeconds, Importance.LOW,
                    "Min retry backoff time for EventHub client")
            .define(CLIENT_RETRY_MAX_BACKOFF, Type.LONG, defaultMaximumBackoffSeconds, Importance.LOW,
                    "Max retry backoff time for EventHub client");

    public EventHubSinkConfig(Map<String, String> configValues) {
        super(CONFIG_DEF, configValues);
    }
}
