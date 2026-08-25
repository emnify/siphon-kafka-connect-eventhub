package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventHubSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(EventHubSinkConnector.class);

    /** Reported when the jar has no {@code Implementation-Version}, e.g. when running from a classes directory. */
    static final String UNKNOWN_VERSION = "unknown";

    private Map<String, String> props;

    @Override
    public String version() {
        String version = getClass().getPackage().getImplementationVersion();
        // Returning null here surfaces as a literal "null" in the Connect REST API, which makes a support
        // conversation about "which build is running" much harder than it needs to be.
        return version != null ? version : UNKNOWN_VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting EventHubSinkConnector {}", version());
        // Validate once, here, so a typo is reported by the REST API at create time instead of by every task.
        new EventHubSinkConfig(props);
        this.props = Collections.unmodifiableMap(new HashMap<>(props));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return EventHubSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating {} task configs", maxTasks);
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            // A fresh copy per task: handing every task the same mutable map invites one task's mutation to be
            // observed by the others.
            taskConfigs.add(new HashMap<>(props));
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Stopping EventHubSinkConnector");
        props = null;
    }

    @Override
    public ConfigDef config() {
        return EventHubSinkConfig.CONFIG_DEF;
    }
}
