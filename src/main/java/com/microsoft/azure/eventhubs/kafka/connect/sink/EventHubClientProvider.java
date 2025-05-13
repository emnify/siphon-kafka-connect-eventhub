package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.microsoft.azure.eventhubs.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class EventHubClientProvider {

    private static final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(4);

    private ConnectionStringBuilder connectionStringBuilder;
    private String authenticationProvider;

    private EventHubClientProvider() {}

    public EventHubClientProvider(String authenticationProvider, String connectionString) {
        this.connectionStringBuilder = new ConnectionStringBuilder(connectionString);
        this.authenticationProvider = authenticationProvider;
    }

    public EventHubClient newInstance() throws EventHubException, IOException {
        if (authenticationProvider == EventHubSinkConfig.JWT_AUTHENTICATION_PROVIDER) {
            return getEventHubClientFromRefreshToken();
        } else {
            return getEventHubClientFromConnectionString();
        }
    }

    public EventHubClient withRetryPolicy(RetryPolicy policy) throws EventHubException, IOException {
        if (authenticationProvider == EventHubSinkConfig.JWT_AUTHENTICATION_PROVIDER) {
            return getEventHubClientFromRefreshToken();
        } else {
            return EventHubClient.createFromConnectionStringSync(this.connectionStringBuilder.toString(), policy, executorService);
        }
    }

    protected EventHubClient getEventHubClientFromRefreshToken() throws IOException {
        try {
            return EventHubClient.createWithTokenProvider(
                    connectionStringBuilder.getEndpoint(),
                    connectionStringBuilder.getEventHubName(),
                    new TokenFilesystemProvider(),
                    executorService,
                    new EventHubClientOptions()
            ).get();
        } catch (IOException ex) {
            throw new IOException("Unable to connect to EventHubs");
        } catch (ExecutionException|InterruptedException ex) {
            throw new IOException("Internal error occurred.");
        }
    }

    protected EventHubClient getEventHubClientFromConnectionString() throws EventHubException, IOException {
        return EventHubClient.createFromConnectionStringSync(this.connectionStringBuilder.toString(), executorService);
    }

}
