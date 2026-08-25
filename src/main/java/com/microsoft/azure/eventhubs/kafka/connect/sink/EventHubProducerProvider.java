package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds {@link EventHubProducerClient} instances from the connector configuration.
 *
 * <p>Each client owns an AMQP connection, so a task builds {@code eventhub.clients.per.task} of them and spreads
 * batches across them. Kept as a separate class so the task can be unit tested without reaching Azure.
 */
class EventHubProducerProvider {

    private static final Logger log = LoggerFactory.getLogger(EventHubProducerProvider.class);

    private final EventHubSinkConfig config;
    private final EventHubConnectionString connectionString;

    /**
     * Shared across every producer this provider builds, so a task with several clients keeps one token cache and
     * reads the token file once per rotation rather than once per client.
     */
    private final FilesystemTokenCredential tokenCredential;

    EventHubProducerProvider(EventHubSinkConfig config) {
        this.config = config;
        this.connectionString = EventHubConnectionString.parse(config.connectionString());
        this.tokenCredential = config.isJwtAuthentication()
                ? FilesystemTokenCredential.forEventHub(config.authTokenDir(), connectionString)
                : null;
    }

    String fullyQualifiedNamespace() {
        return connectionString.fullyQualifiedNamespace();
    }

    String entityPath() {
        return connectionString.entityPath();
    }

    /**
     * Opens one producer client.
     *
     * <p>Failures here are not translated: {@code start()} decides whether an unreachable Event Hub should fail the
     * task outright or be retried, and it needs the original exception to do that.
     */
    EventHubProducerClient newProducer() {
        // Never log the connection string itself - it carries the shared access key.
        log.info("Opening Event Hubs producer [namespace={}, eventHub={}, auth={}, transport={}]",
                connectionString.fullyQualifiedNamespace(), connectionString.entityPath(),
                config.getString(EventHubSinkConfig.AUTHENTICATION_PROVIDER), config.transportType());

        EventHubClientBuilder builder = new EventHubClientBuilder()
                .retryOptions(config.retryOptions())
                .transportType(config.transportType());

        if (config.isJwtAuthentication()) {
            builder.credential(connectionString.fullyQualifiedNamespace(), connectionString.entityPath(),
                    tokenCredential);
        } else {
            builder.connectionString(config.connectionString());
        }

        return builder.buildProducerClient();
    }
}
