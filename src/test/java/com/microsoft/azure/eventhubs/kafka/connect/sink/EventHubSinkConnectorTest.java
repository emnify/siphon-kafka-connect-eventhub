package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;

public class EventHubSinkConnectorTest {

    private EventHubSinkConnector connector;
    private Map<String, String> props;

    @Before
    public void setUp() {
        connector = new EventHubSinkConnector();
        props = new HashMap<>();
        props.put(EventHubSinkConfig.CONNECTION_STRING,
                "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=k;EntityPath=hub");
    }

    @Test
    public void versionIsNeverNull() {
        // A null here shows up as the literal string "null" in the Connect REST API.
        assertNotNull(connector.version());
    }

    @Test
    public void taskClassIsTheSinkTask() {
        assertEquals(EventHubSinkTask.class, connector.taskClass());
    }

    @Test
    public void configIsExposedForTheRestApi() {
        assertNotNull(connector.config().configKeys().get(EventHubSinkConfig.CONNECTION_STRING));
    }

    @Test
    public void invalidConfigIsRejectedAtCreateTimeRatherThanByEveryTask() {
        props.put(EventHubSinkConfig.AUTHENTICATION_PROVIDER, "OAUTH");
        assertThrows(ConfigException.class, () -> connector.start(props));
    }

    @Test
    public void eachTaskGetsItsOwnConfigCopy() {
        connector.start(props);
        List<Map<String, String>> configs = connector.taskConfigs(3);

        assertEquals(3, configs.size());
        assertEquals(props, configs.get(0));
        assertNotSame(configs.get(0), configs.get(1));
    }

    @Test
    public void mutatingTheCallersMapAfterStartDoesNotChangeTaskConfigs() {
        connector.start(props);
        props.put(EventHubSinkConfig.CLIENTS_PER_TASK, "9");

        assertEquals(null, connector.taskConfigs(1).get(0).get(EventHubSinkConfig.CLIENTS_PER_TASK));
    }
}
