package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.core.amqp.AmqpRetryMode;
import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.core.amqp.AmqpTransportType;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.types.Password;

import java.time.Duration;
import java.util.Map;

public class EventHubSinkConfig extends AbstractConfig {

    /**
     * The connection string for the Event Hub. This can be retrieved from the
     * Azure Portal -&gt; Event Hubs -&gt; Your Event Hub -&gt; Overview -&gt; Connection Strings.
     *
     * <p>Declared as {@link Type#PASSWORD} so that the Connect REST API and Connect's own logging mask it.
     */
    public static final String CONNECTION_STRING = "eventhub.connection.string";

    /**
     * The number of Azure Event Hubs producer clients to use per task. Each client opens its own AMQP connection,
     * which helps with gaining more throughput.
     */
    public static final String CLIENTS_PER_TASK = "eventhub.clients.per.task";

    /**
     * Event Hubs serialization type for structured events with schema. Currently only {@code json} is implemented.
     */
    public static final String SERIALIZATION_TYPE = "eventhub.serialization";

    /** Event Hubs client retry policy - retry count. */
    public static final String CLIENT_RETRY_COUNT = "eventhub.client.retry.count";

    /** Event Hubs client retry policy - minimum backoff, in seconds. */
    public static final String CLIENT_RETRY_MIN_BACKOFF = "eventhub.client.retry.minimumBackoff";

    /** Event Hubs client retry policy - maximum backoff, in seconds. */
    public static final String CLIENT_RETRY_MAX_BACKOFF = "eventhub.client.retry.maximumBackoff";

    /** Event Hubs client retry policy - backoff mode, {@code EXPONENTIAL} or {@code FIXED}. */
    public static final String CLIENT_RETRY_MODE = "eventhub.client.retry.mode";

    /**
     * Per-attempt timeout, in seconds. Bounds how long a single send attempt may block before the SDK retries it.
     * Total worst-case time for one send is roughly {@code (retry.count + 1) * try.timeout + sum(backoffs)}.
     */
    public static final String CLIENT_TRY_TIMEOUT = "eventhub.client.try.timeout";

    /**
     * Overall deadline, in seconds, for a single {@code put()} call to finish sending its batch. Keep this
     * comfortably below the worker's {@code consumer.max.poll.interval.ms} so a slow Event Hub cannot cause a
     * consumer group rebalance.
     */
    public static final String SEND_TIMEOUT = "eventhub.send.timeout";

    /** AMQP transport, {@code AMQP} (port 5671) or {@code AMQP_WEB_SOCKETS} (port 443, firewall friendly). */
    public static final String TRANSPORT_TYPE = "eventhub.transport.type";

    /** Event Hubs authentication provider. Valid values are {@code SAS} or {@code JWT} from the filesystem. */
    public static final String AUTHENTICATION_PROVIDER = "eventhub.authentication";

    /**
     * Directory holding filesystem JWTs, used when {@link #AUTHENTICATION_PROVIDER} is {@code JWT}. The token is
     * read from {@code <dir>/<namespace>/<entityPath>}. Defaults to the {@code CONNECT_AUTH_TOKEN_DIR} environment
     * variable so that existing deployments keep working.
     */
    public static final String AUTH_TOKEN_DIR = "eventhub.auth.token.dir";

    /**
     * Whether {@code start()} proves the connection before reporting the task as running.
     *
     * <p>Building a v5 producer client opens no connection, so without this probe a task with a bad credential or
     * a misspelled Event Hub reports RUNNING and only fails when the first record arrives - which may be hours
     * later, on a different shift.
     */
    public static final String VALIDATE_ON_START = "eventhub.validate.on.start";

    /**
     * How long, in seconds, {@code start()} keeps retrying a <em>transient</em> startup failure before giving up.
     * Configuration failures are not retried at all, however long this is.
     */
    public static final String START_TIMEOUT = "eventhub.start.timeout";

    /**
     * Where the Event Hubs partition key comes from. {@code none} (default) lets Event Hubs round-robin events
     * across partitions; {@code record.key} derives the partition key from the Kafka record key, which preserves
     * per-key ordering at the cost of uneven partition load.
     */
    public static final String PARTITION_KEY_SOURCE = "eventhub.partition.key.source";

    /** Event Hubs SAS (default) authentication provider. */
    public static final String SAS_AUTHENTICATION_PROVIDER = "SAS";

    /** Event Hubs filesystem-JWT authentication provider. */
    public static final String JWT_AUTHENTICATION_PROVIDER = "JWT";

    /** Send all structured events to Event Hubs as JSON. */
    public static final String SERIALIZATION_JSON = "json";

    /** Do not set a partition key; Event Hubs distributes events across partitions. */
    public static final String PARTITION_KEY_NONE = "none";

    /** Derive the Event Hubs partition key from the Kafka record key. */
    public static final String PARTITION_KEY_RECORD_KEY = "record.key";

    /** Environment variable consulted for {@link #AUTH_TOKEN_DIR} when the property is not set. */
    static final String AUTH_TOKEN_DIR_ENV = "CONNECT_AUTH_TOKEN_DIR";

    private static final short DEFAULT_CLIENTS_PER_TASK = 1;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_MIN_BACKOFF_SECONDS = 1;
    private static final long DEFAULT_MAX_BACKOFF_SECONDS = 30;
    /**
     * These four defaults are chosen together, and changing one in isolation is a trap. The client's worst case is
     * {@code (retries + 1) * tryTimeout + retries * maxBackoff} = {@code 4 * 30 + 3 * 30} = 210s. That has to fit
     * inside {@link #SEND_TIMEOUT} (240s), which in turn has to fit inside Kafka's default
     * {@code max.poll.interval.ms} of 300s - otherwise a slow Event Hub triggers a consumer group rebalance
     * instead of a retry. {@code EventHubSinkTask} re-checks this at startup and warns when a custom config
     * inverts it.
     */
    private static final long DEFAULT_TRY_TIMEOUT_SECONDS = 30;
    private static final long DEFAULT_SEND_TIMEOUT_SECONDS = 240;
    private static final long DEFAULT_START_TIMEOUT_SECONDS = 60;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONNECTION_STRING, Type.PASSWORD, Importance.HIGH,
                    "Event Hubs connection string, including EntityPath for the target Event Hub")
            .define(CLIENTS_PER_TASK, Type.SHORT, DEFAULT_CLIENTS_PER_TASK, Range.atLeast((short) 1), Importance.HIGH,
                    "Number of Event Hubs producer clients (AMQP connections) to use per task")
            .define(SERIALIZATION_TYPE, Type.STRING, SERIALIZATION_JSON, ValidString.in(SERIALIZATION_JSON), Importance.LOW,
                    "Serialization applied to structured events with a schema. Only 'json' is implemented")
            .define(AUTHENTICATION_PROVIDER, Type.STRING, SAS_AUTHENTICATION_PROVIDER,
                    ValidString.in(SAS_AUTHENTICATION_PROVIDER, JWT_AUTHENTICATION_PROVIDER), Importance.HIGH,
                    "Method of Event Hubs authentication: SAS connection string, or JWT read from the filesystem")
            .define(AUTH_TOKEN_DIR, Type.STRING, null, Importance.MEDIUM,
                    "Directory holding filesystem JWTs when " + AUTHENTICATION_PROVIDER + "=" + JWT_AUTHENTICATION_PROVIDER
                            + ". Defaults to the " + AUTH_TOKEN_DIR_ENV + " environment variable")
            .define(CLIENT_RETRY_COUNT, Type.INT, DEFAULT_MAX_RETRIES, Range.atLeast(0), Importance.LOW,
                    "Maximum number of in-client retries of a transient failure before it is surfaced to the task")
            .define(CLIENT_RETRY_MIN_BACKOFF, Type.LONG, DEFAULT_MIN_BACKOFF_SECONDS, Range.atLeast(0L), Importance.LOW,
                    "Minimum retry backoff, in seconds, for the Event Hubs client")
            .define(CLIENT_RETRY_MAX_BACKOFF, Type.LONG, DEFAULT_MAX_BACKOFF_SECONDS, Range.atLeast(0L), Importance.LOW,
                    "Maximum retry backoff, in seconds, for the Event Hubs client")
            .define(CLIENT_RETRY_MODE, Type.STRING, AmqpRetryMode.EXPONENTIAL.name(),
                    ValidString.in(AmqpRetryMode.EXPONENTIAL.name(), AmqpRetryMode.FIXED.name()), Importance.LOW,
                    "Retry backoff mode for the Event Hubs client")
            .define(CLIENT_TRY_TIMEOUT, Type.LONG, DEFAULT_TRY_TIMEOUT_SECONDS, Range.atLeast(1L), Importance.MEDIUM,
                    "Timeout, in seconds, of a single send attempt before the client retries it")
            .define(SEND_TIMEOUT, Type.LONG, DEFAULT_SEND_TIMEOUT_SECONDS, Range.atLeast(1L), Importance.MEDIUM,
                    "Overall deadline, in seconds, for one put() to deliver its batch. Keep below max.poll.interval.ms")
            .define(TRANSPORT_TYPE, Type.STRING, AmqpTransportType.AMQP.name(),
                    ValidString.in(AmqpTransportType.AMQP.name(), AmqpTransportType.AMQP_WEB_SOCKETS.name()), Importance.MEDIUM,
                    "AMQP transport: AMQP over port 5671, or AMQP_WEB_SOCKETS over port 443")
            .define(VALIDATE_ON_START, Type.BOOLEAN, true, Importance.MEDIUM,
                    "Prove the connection during start() so bad credentials or a missing Event Hub fail the task "
                            + "immediately instead of on the first record")
            .define(START_TIMEOUT, Type.LONG, DEFAULT_START_TIMEOUT_SECONDS, Range.atLeast(0L), Importance.LOW,
                    "How long, in seconds, start() retries a transient startup failure. Configuration failures are "
                            + "never retried")
            .define(PARTITION_KEY_SOURCE, Type.STRING, PARTITION_KEY_NONE,
                    ValidString.in(PARTITION_KEY_NONE, PARTITION_KEY_RECORD_KEY), Importance.MEDIUM,
                    "Source of the Event Hubs partition key: 'none' or 'record.key'");

    public EventHubSinkConfig(Map<String, String> configValues) {
        super(CONFIG_DEF, configValues);
    }

    /** The connection string in clear text. Never log the result. */
    public String connectionString() {
        Password password = getPassword(CONNECTION_STRING);
        return password == null ? null : password.value();
    }

    public short clientsPerTask() {
        return getShort(CLIENTS_PER_TASK);
    }

    public boolean isJwtAuthentication() {
        // String#equals, not ==: the value arrives from a parsed config map and is not interned.
        return JWT_AUTHENTICATION_PROVIDER.equals(getString(AUTHENTICATION_PROVIDER));
    }

    public boolean isPartitionKeyFromRecordKey() {
        return PARTITION_KEY_RECORD_KEY.equals(getString(PARTITION_KEY_SOURCE));
    }

    /** Directory holding filesystem JWTs, falling back to the legacy environment variable. */
    public String authTokenDir() {
        String configured = getString(AUTH_TOKEN_DIR);
        return configured != null && !configured.trim().isEmpty() ? configured.trim() : System.getenv(AUTH_TOKEN_DIR_ENV);
    }

    public AmqpTransportType transportType() {
        return AmqpTransportType.valueOf(getString(TRANSPORT_TYPE));
    }

    public Duration sendTimeout() {
        return Duration.ofSeconds(getLong(SEND_TIMEOUT));
    }

    public boolean validateOnStart() {
        return getBoolean(VALIDATE_ON_START);
    }

    public Duration startTimeout() {
        return Duration.ofSeconds(getLong(START_TIMEOUT));
    }

    public AmqpRetryOptions retryOptions() {
        // AmqpRetryOptions rejects a maxDelay below delay, so clamp rather than fail the task at startup.
        Duration delay = Duration.ofSeconds(getLong(CLIENT_RETRY_MIN_BACKOFF));
        Duration maxDelay = Duration.ofSeconds(getLong(CLIENT_RETRY_MAX_BACKOFF));
        if (maxDelay.compareTo(delay) < 0) {
            maxDelay = delay;
        }
        return new AmqpRetryOptions()
                .setMode(AmqpRetryMode.valueOf(getString(CLIENT_RETRY_MODE)))
                .setMaxRetries(getInt(CLIENT_RETRY_COUNT))
                .setDelay(delay)
                .setMaxDelay(maxDelay)
                .setTryTimeout(Duration.ofSeconds(getLong(CLIENT_TRY_TIMEOUT)));
    }
}
