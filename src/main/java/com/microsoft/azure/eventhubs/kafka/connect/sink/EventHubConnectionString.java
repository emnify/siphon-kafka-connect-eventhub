package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/**
 * Parses the components of an Event Hubs connection string.
 *
 * <p>The v5 SDK hands the whole connection string to {@code EventHubClientBuilder.connectionString(..)}, but the
 * token-credential path needs the fully qualified namespace and the entity path separately. The SDK only exposes a
 * parser in an {@code implementation} package, so this connector parses the string itself rather than depending on
 * an internal API.
 *
 * <p>Expected form:
 * {@code Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=..;SharedAccessKey=..;EntityPath=..}
 */
final class EventHubConnectionString {

    private final String fullyQualifiedNamespace;
    private final String entityPath;

    private EventHubConnectionString(String fullyQualifiedNamespace, String entityPath) {
        this.fullyQualifiedNamespace = fullyQualifiedNamespace;
        this.entityPath = entityPath;
    }

    String fullyQualifiedNamespace() {
        return fullyQualifiedNamespace;
    }

    /** The Event Hub (topic) name, or {@code null} when the connection string is namespace-scoped. */
    String entityPath() {
        return entityPath;
    }

    static EventHubConnectionString parse(String connectionString) {
        if (connectionString == null || connectionString.trim().isEmpty()) {
            throw new ConfigException(EventHubSinkConfig.CONNECTION_STRING, "<hidden>", "must not be empty");
        }

        String endpoint = null;
        String entityPath = null;
        for (String token : connectionString.split(";")) {
            String pair = token.trim();
            if (pair.isEmpty()) {
                continue;
            }
            int separator = pair.indexOf('=');
            if (separator < 1) {
                continue;
            }
            String key = pair.substring(0, separator).trim().toLowerCase(Locale.ROOT);
            // Values may legitimately contain '=' (base64 padding on SharedAccessKey), so only split on the first one.
            String value = pair.substring(separator + 1).trim();
            if ("endpoint".equals(key)) {
                endpoint = value;
            } else if ("entitypath".equals(key)) {
                entityPath = value;
            }
        }

        if (endpoint == null) {
            throw new ConfigException(EventHubSinkConfig.CONNECTION_STRING, "<hidden>", "is missing the 'Endpoint' component");
        }

        String host;
        try {
            host = new URI(endpoint).getHost();
        } catch (URISyntaxException ex) {
            throw new ConfigException(EventHubSinkConfig.CONNECTION_STRING, "<hidden>",
                    "has an 'Endpoint' component that is not a valid URI");
        }
        if (host == null || host.isEmpty()) {
            throw new ConfigException(EventHubSinkConfig.CONNECTION_STRING, "<hidden>",
                    "has an 'Endpoint' component without a host");
        }

        return new EventHubConnectionString(host, entityPath == null || entityPath.isEmpty() ? null : entityPath);
    }
}
