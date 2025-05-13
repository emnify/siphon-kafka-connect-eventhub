package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.microsoft.azure.eventhubs.impl.RetryExponential;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import org.apache.kafka.connect.errors.ConnectException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import com.microsoft.azure.eventhubs.*;

public class EventHubSinkTask extends SinkTask {
    // List of EventHubClient objects to be used during data upload
    private BlockingQueue<EventHubClient> ehClients;
    private EventDataExtractor extractor = new EventDataExtractor();
    private EventHubClientProvider provider;

    private static final Logger log = LoggerFactory.getLogger(EventHubSinkTask.class);

    public String version() {
        return new EventHubSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("starting EventHubSinkTask");
        EventHubSinkConfig eventHubSinkConfig;
        try {
            eventHubSinkConfig = new EventHubSinkConfig(props);
        } catch (ConfigException ex) {
            throw new ConnectException("Couldn't start EventHubSinkTask due to configuration error", ex);
        }

        String connectionString = eventHubSinkConfig.getString(EventHubSinkConfig.CONNECTION_STRING);
        log.info("connection string = {}", connectionString);
        short clientsPerTask = eventHubSinkConfig.getShort(EventHubSinkConfig.CLIENTS_PER_TASK);
        log.info("clients per task = {}", clientsPerTask);
        String authType = eventHubSinkConfig.getString(EventHubSinkConfig.AUTHENTICATION_PROVIDER);
        log.info("client auth provider = {}", authType);

        initializeEventHubClients(authType,
                connectionString,
                clientsPerTask,
                eventHubSinkConfig.getInt(EventHubSinkConfig.CLIENT_RETRY_COUNT),
                Duration.ofSeconds(eventHubSinkConfig.getLong(EventHubSinkConfig.CLIENT_RETRY_MIN_BACKOFF)),
                Duration.ofSeconds(eventHubSinkConfig.getLong(EventHubSinkConfig.CLIENT_RETRY_MAX_BACKOFF))
        );
        extractor.configureJson(props);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        log.debug("starting to upload {} records", sinkRecords.size());
        List<CompletableFuture<Void>> resultSet = new LinkedList<>();
        for (SinkRecord record : sinkRecords) {
            EventData sendEvent = null;
            EventHubClient ehClient = null;
            try {
                sendEvent = extractor.extractEventData(record);
                // pick an event hub client to send the data asynchronously
                ehClient = ehClients.take();
                if (sendEvent != null)
                    resultSet.add(sendAsync(ehClient, sendEvent));
            } catch (InterruptedException ex) {
                throw new ConnectException("EventHubSinkTask interrupted while waiting to acquire client", ex);
            }
            finally {
                if(ehClient != null) {
                    ehClients.offer(ehClient);
                }
            }
        }

        log.debug("wait for {} async uploads to finish", resultSet.size());
        waitForAllUploads(resultSet);
        log.debug("finished uploading {} records", sinkRecords.size());
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
        log.info("stopping EventHubSinkTask");
        if(ehClients != null) {
            for (EventHubClient ehClient : ehClients) {
                ehClient.close();
                log.info("closing an Event hub Client");
            }
        }
    }

    protected CompletableFuture<Void> sendAsync(EventHubClient ehClient, EventData sendEvent) {
        return ehClient.send(sendEvent);
    }


    protected int getClientCount() {
        if(ehClients != null) {
            return ehClients.size();
        }
        else {
            return 0;
        }
    }

    private void initializeEventHubClients(String authType,
                                           String connectionString,
                                           short clientsPerTask,
                                           int retryCount,
                                           Duration minBackoff,
                                           Duration maxBackoff) {
        ehClients = new LinkedBlockingQueue<EventHubClient>(clientsPerTask);
        provider = getClientProvider(authType, connectionString);
        try {
            for (short i = 0; i < clientsPerTask; i++) {
                log.info("Creating event hub client authType=%s, connectionString=%s", authType, connectionString);
                ehClients.offer(provider.withRetryPolicy(new RetryExponential(
                        minBackoff,
                        maxBackoff,
                        retryCount,
                        "Retry policy for EventHub client"
                )));
                log.info("Created an Event Hub Client");
            }
        } catch (AuthorizationFailedException ex) {
            log.error("Authorization failed while connecting to EventHub [connectionString=%s]", connectionString);
            throw new ConnectException("Authorization error. Unable to connect to EventHub", ex);
        } catch (EventHubException ex) {
            log.error("Error occurred while connecting to EventHub [connectionString=%s]", connectionString);
            throw new ConnectException("Exception while creating Event Hub client: " + ex.getMessage(), ex);
        } catch (IOException ex) {
            throw new ConnectException("Error while connecting to EventHubs", ex);
        }
    }

    protected EventHubClientProvider getClientProvider(String authType, String connectionString) {
        return new EventHubClientProvider(authType, connectionString);
    }

    private void waitForAllUploads(List<CompletableFuture<Void>> resultSet) {
        for(CompletableFuture<Void> result : resultSet) {
            try {
                try {
                    result.get();
                } catch (ExecutionException|InterruptedException ex) {
                    findValidRootCause(ex);
                }
            } catch (AuthorizationFailedException ex) {
                log.error("Authorization failed while sending events to EventHub");
                throw new ConnectException("Authorization error. Unable to connect to EventHub", ex);
            } catch (EventHubException ex) {
                log.error("Error occurred while sending events to EventHub");
                throw new ConnectException("Exception while creating Event Hub client: " + ex.getMessage(), ex);
            } catch (IOException ex) {
                throw new ConnectException("Error while connecting to EventHubs", ex);
            }
        }
    }

    private void findValidRootCause(Exception ex) throws EventHubException, IOException {
        Throwable rootCause = ex;
        while (rootCause != null) {
            rootCause = rootCause.getCause();
            if (rootCause instanceof IOException) {
                throw (IOException) rootCause;
            } else if (rootCause instanceof EventHubException) {
                throw (EventHubException) rootCause;
            }
        }
        throw new IOException("Error while executing send" , ex);
    }
}
