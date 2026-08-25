package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class EventHubConnectionStringTest {

    private static final String FULL =
            "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=aGVsbG8=;EntityPath=hub";

    @Test
    public void extractsNamespaceAndEntityPath() {
        EventHubConnectionString parsed = EventHubConnectionString.parse(FULL);
        assertEquals("ns.servicebus.windows.net", parsed.fullyQualifiedNamespace());
        assertEquals("hub", parsed.entityPath());
    }

    @Test
    public void keysContainingEqualsSignsSurviveParsing() {
        // Base64 shared access keys are routinely padded with '=', so only the first '=' separates key from value.
        EventHubConnectionString parsed = EventHubConnectionString.parse(
                "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=YWJjZGVmZ2g==;EntityPath=hub");
        assertEquals("hub", parsed.entityPath());
    }

    @Test
    public void componentNamesAreCaseInsensitive() {
        EventHubConnectionString parsed = EventHubConnectionString.parse(
                "endpoint=sb://ns.servicebus.windows.net/;entitypath=hub");
        assertEquals("ns.servicebus.windows.net", parsed.fullyQualifiedNamespace());
        assertEquals("hub", parsed.entityPath());
    }

    @Test
    public void namespaceScopedConnectionStringHasNoEntityPath() {
        assertNull(EventHubConnectionString.parse(
                "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=k").entityPath());
    }

    @Test
    public void emptyEntityPathIsTreatedAsAbsent() {
        assertNull(EventHubConnectionString.parse(
                "Endpoint=sb://ns.servicebus.windows.net/;EntityPath=").entityPath());
    }

    @Test
    public void rejectsMissingEndpoint() {
        assertThrows(ConfigException.class, () -> EventHubConnectionString.parse("EntityPath=hub"));
    }

    @Test
    public void rejectsBlankConnectionString() {
        assertThrows(ConfigException.class, () -> EventHubConnectionString.parse("   "));
        assertThrows(ConfigException.class, () -> EventHubConnectionString.parse(null));
    }

    @Test
    public void errorMessagesNeverEchoTheConnectionString() {
        // The connection string carries the shared access key; a ConfigException message reaches the REST API.
        ConfigException ex = assertThrows(ConfigException.class,
                () -> EventHubConnectionString.parse("SharedAccessKey=super-secret-key"));
        assertEquals(-1, ex.getMessage().indexOf("super-secret-key"));
    }
}
