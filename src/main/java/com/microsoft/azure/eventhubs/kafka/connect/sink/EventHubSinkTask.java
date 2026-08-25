package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sink task that publishes Kafka records to an Azure Event Hub.
 *
 * <p><b>Delivery semantics: at-least-once.</b> {@code put()} blocks until every event in the batch is acknowledged
 * by Event Hubs, so {@code flush()} has nothing left to do. If a send fails after some batches were already
 * accepted, the whole {@code put()} is retried and those batches are delivered twice. Event Hubs offers no
 * idempotent producer, so downstream consumers must be able to tolerate duplicates.
 *
 * <p><b>Failure handling</b> splits three ways:
 * <ul>
 *   <li>a record that cannot be serialized goes to the dead letter queue, if one is configured, and the task
 *       carries on;</li>
 *   <li>a transient Event Hubs failure raises {@link RetriableException} so Connect re-delivers the batch;</li>
 *   <li>anything else fails the task, because retrying it would just spin.</li>
 * </ul>
 */
public class EventHubSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(EventHubSinkTask.class);

    private final EventDataExtractor extractor = new EventDataExtractor();

    private List<EventHubProducerClient> producers = Collections.emptyList();
    private ExecutorService sendExecutor;
    private EventHubSinkConfig config;
    private Duration sendTimeout;
    private Duration startTimeout;
    private boolean validateOnStart;
    private boolean partitionKeyFromRecordKey;
    private ErrantRecordReporter errantRecordReporter;

    @Override
    public String version() {
        return new EventHubSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            doStart(props);
        } catch (RetriableException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            throw startupFailure(ex);
        }
    }

    /**
     * Re-wraps a startup failure so the message survives all the way to the customer.
     *
     * <p>Kafka Connect wraps a {@code put()} failure in a {@code ConnectException} before recording the task's
     * stack trace, but it records a {@code start()} failure verbatim. ecds-api only recognises a message on a
     * {@code Caused by: org.apache.kafka.connect.errors.ConnectException:} line, so an unwrapped startup failure
     * has no such line and is reported as "Error state: Unknown error" - precisely for the misconfigurations an
     * operator most needs to see. Wrapping here restores the shape the extractor expects.
     */
    private static ConnectException startupFailure(RuntimeException ex) {
        ConnectException reportable = ex instanceof ConnectException && ex.getClass() == ConnectException.class
                ? (ConnectException) ex
                : new ConnectException(ex.getMessage(), ex);
        return new ConnectException("EventHubSinkTask failed to start. " + reportable.getMessage(), reportable);
    }

    private void doStart(Map<String, String> props) {
        log.info("Starting EventHubSinkTask");
        try {
            config = new EventHubSinkConfig(props);
        } catch (ConfigException ex) {
            throw new ConnectException("[EVENTHUB_CONFIG] Invalid connector configuration: " + ex.getMessage(), ex);
        }

        sendTimeout = config.sendTimeout();
        startTimeout = config.startTimeout();
        validateOnStart = config.validateOnStart();
        partitionKeyFromRecordKey = config.isPartitionKeyFromRecordKey();
        errantRecordReporter = resolveErrantRecordReporter();

        short clientsPerTask = config.clientsPerTask();
        AmqpRetryOptions retry = config.retryOptions();
        log.info("EventHubSinkTask configuration [clientsPerTask={}, auth={}, transport={}, partitionKeySource={}, "
                        + "retry={}x, tryTimeout={}, sendTimeout={}, deadLetterQueue={}]",
                clientsPerTask, config.getString(EventHubSinkConfig.AUTHENTICATION_PROVIDER), config.transportType(),
                config.getString(EventHubSinkConfig.PARTITION_KEY_SOURCE), retry.getMaxRetries(),
                retry.getTryTimeout(), sendTimeout, errantRecordReporter != null ? "enabled" : "disabled");

        // Configure the converter first: a bad json.*.pattern must fail before any AMQP connection is opened,
        // because Connect does not reliably call stop() after start() throws.
        extractor.configureJson(props);
        warnIfSendTimeoutIsTooTight();
        initializeProducers(clientsPerTask);
    }

    /**
     * Warns when {@code eventhub.send.timeout} is shorter than the SDK's own worst-case retry budget.
     *
     * <p>Getting this wrong is the classic way to turn intermittent errors into a permanent outage: the task
     * abandons every batch on the Connect-level deadline before the SDK has finished retrying, so throttling that
     * would have cleared on its own becomes an endless retry loop with no progress.
     */
    private void warnIfSendTimeoutIsTooTight() {
        AmqpRetryOptions retry = config.retryOptions();
        Duration worstCase = retry.getTryTimeout()
                .multipliedBy(retry.getMaxRetries() + 1L)
                .plus(retry.getMaxDelay().multipliedBy(retry.getMaxRetries()));
        if (sendTimeout.compareTo(worstCase) < 0) {
            log.warn("{}={}s is below the client's worst-case retry budget of {}s ({}={} means up to {} attempts of "
                            + "{}s each, plus up to {}s of backoff). Batches may be abandoned before the client has "
                            + "finished retrying them, so a transient failure never gets the chance to clear.",
                    EventHubSinkConfig.SEND_TIMEOUT, sendTimeout.getSeconds(), worstCase.getSeconds(),
                    EventHubSinkConfig.CLIENT_RETRY_COUNT, retry.getMaxRetries(), retry.getMaxRetries() + 1,
                    retry.getTryTimeout().getSeconds(), retry.getMaxDelay().multipliedBy(retry.getMaxRetries()).getSeconds());
        }
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        if (sinkRecords.isEmpty()) {
            return;
        }
        log.debug("Uploading {} records", sinkRecords.size());

        List<PendingBatch> batches = buildBatches(sinkRecords);
        if (batches.isEmpty()) {
            log.debug("Nothing to send after conversion of {} records", sinkRecords.size());
            return;
        }

        sendAll(batches);
        log.debug("Uploaded {} records in {} batches", sinkRecords.size(), batches.size());
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // Intentionally empty: put() does not return until Event Hubs has acknowledged the batch, so an offset that
        // Connect is about to commit is already durable.
    }

    @Override
    public void stop() {
        log.info("Stopping EventHubSinkTask");
        shutdownExecutor();
        for (EventHubProducerClient producer : producers) {
            try {
                producer.close();
            } catch (RuntimeException ex) {
                // A failure to close is not worth failing a shutdown over, but silence would hide a leak.
                log.warn("Error closing Event Hubs producer: {}", EventHubErrors.describe(ex));
            }
        }
        producers = Collections.emptyList();
    }

    /** Visible for testing. */
    protected EventHubProducerProvider getProducerProvider(EventHubSinkConfig config) {
        return new EventHubProducerProvider(config);
    }

    /** Visible for testing. */
    protected int getClientCount() {
        return producers.size();
    }

    /**
     * Converts records to events and packs them into Event Hubs batches.
     *
     * <p>Batching is the single largest throughput difference from the previous implementation, which issued one
     * AMQP send per record. A record that does not fit in an empty batch is larger than the Event Hub's maximum
     * message size and can never be sent, so it is dead lettered rather than retried forever.
     */
    private List<PendingBatch> buildBatches(Collection<SinkRecord> sinkRecords) {
        // One open batch per partition key, so records sharing a key land in the same Event Hubs partition in
        // order. With the default 'none' key source there is a single null-keyed batch.
        Map<String, PendingBatch> open = new LinkedHashMap<>();
        List<PendingBatch> completed = new ArrayList<>();
        int producerIndex = 0;

        for (SinkRecord record : sinkRecords) {
            EventData event;
            try {
                event = extractor.extractEventData(record);
            } catch (DataException ex) {
                reportBadRecord(record, ex);
                continue;
            }
            if (event == null) {
                continue;
            }

            String partitionKey = partitionKeyFromRecordKey ? EventDataExtractor.partitionKeyOf(record) : null;
            PendingBatch current = open.get(partitionKey);
            if (current == null) {
                current = newBatch(partitionKey, producerIndex++);
                open.put(partitionKey, current);
            }

            if (current.batch.tryAdd(event)) {
                current.records.add(record);
                continue;
            }

            if (current.batch.getCount() == 0) {
                // The event does not fit in an empty batch, so it exceeds the Event Hub's maximum message size and
                // will never be deliverable. Retrying it would block the partition forever.
                reportBadRecord(record, oversizedRecord(current.batch.getMaxSizeInBytes()));
                continue;
            }

            // Batch is full: seal it and open a fresh one for the same partition key.
            completed.add(current);
            PendingBatch next = newBatch(partitionKey, producerIndex++);
            open.put(partitionKey, next);
            if (next.batch.tryAdd(event)) {
                next.records.add(record);
            } else {
                reportBadRecord(record, oversizedRecord(next.batch.getMaxSizeInBytes()));
            }
        }

        for (PendingBatch pending : open.values()) {
            if (pending.batch.getCount() > 0) {
                completed.add(pending);
            }
        }
        return completed;
    }

    private static DataException oversizedRecord(int maxSizeInBytes) {
        return new DataException("Record does not fit in an empty Event Hubs batch (max " + maxSizeInBytes
                + " bytes); it exceeds the Event Hub's maximum message size and can never be delivered");
    }

    private PendingBatch newBatch(String partitionKey, int producerIndex) {
        EventHubProducerClient producer = producers.get(Math.floorMod(producerIndex, producers.size()));
        CreateBatchOptions options = new CreateBatchOptions();
        if (partitionKey != null) {
            options.setPartitionKey(partitionKey);
        }
        try {
            return new PendingBatch(producer, producer.createBatch(options), partitionKey);
        } catch (RuntimeException ex) {
            // createBatch() talks to the service to learn the maximum frame size, so it can fail transiently.
            throw EventHubErrors.toConnectException(
                    "Unable to create an Event Hubs batch: " + EventHubErrors.describe(ex), ex);
        }
    }

    /**
     * Sends every batch and waits for all of them, then decides what to throw.
     *
     * <p>Waiting for all of them matters: bailing out on the first failure would leave the remaining sends running
     * against producers that {@code stop()} is about to close, and would report whichever failure happened to
     * arrive first rather than the most actionable one. A permanent failure always wins over a transient one, so
     * one poison batch cannot hide behind a concurrent throttling response and retry forever.
     */
    private void sendAll(List<PendingBatch> batches) {
        List<CompletableFuture<Void>> inFlight = new ArrayList<>(batches.size());
        for (PendingBatch pending : batches) {
            inFlight.add(CompletableFuture.runAsync(() -> pending.producer.send(pending.batch), sendExecutor));
        }

        Throwable retriable = null;
        PendingBatch retriableBatch = null;
        Throwable permanent = null;
        PendingBatch permanentBatch = null;
        long deadlineNanos = System.nanoTime() + sendTimeout.toNanos();

        for (int i = 0; i < inFlight.size(); i++) {
            try {
                long remainingNanos = deadlineNanos - System.nanoTime();
                inFlight.get(i).get(Math.max(0L, remainingNanos), TimeUnit.NANOSECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                cancel(inFlight);
                throw new RetriableException("Interrupted while sending to Event Hubs", ex);
            } catch (TimeoutException ex) {
                long unacknowledged = inFlight.stream().filter(future -> !future.isDone()).count();
                cancel(inFlight);
                throw new RetriableException("Sending to Event Hubs exceeded " + EventHubSinkConfig.SEND_TIMEOUT
                        + "=" + sendTimeout.getSeconds() + "s with " + unacknowledged + " of " + inFlight.size()
                        + " batches unacknowledged", ex);
            } catch (ExecutionException | CompletionException ex) {
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                if (EventHubErrors.isRetriable(cause)) {
                    if (retriable == null) {
                        retriable = cause;
                        retriableBatch = batches.get(i);
                    }
                } else if (permanent == null) {
                    permanent = cause;
                    permanentBatch = batches.get(i);
                }
            }
        }

        if (permanent != null) {
            String diagnosis = EventHubErrors.diagnose(permanent);
            if (diagnosis != null) {
                throw new ConnectException(diagnosis + " Rejected " + permanentBatch + ", reported by Event Hubs as "
                        + EventHubErrors.describe(permanent), permanent);
            }
            throw new ConnectException("Permanent failure sending to Event Hubs for " + permanentBatch
                    + ": " + EventHubErrors.describe(permanent), permanent);
        }
        if (retriable != null) {
            log.warn("Transient failure sending to Event Hubs for {}, the batch will be retried: {}",
                    retriableBatch, EventHubErrors.describe(retriable));
            throw new RetriableException("Transient failure sending to Event Hubs for " + retriableBatch
                    + ": " + EventHubErrors.describe(retriable), retriable);
        }
    }

    /**
     * Marks the outstanding futures cancelled.
     *
     * <p>This does not interrupt a send that is already running - {@code CompletableFuture.cancel} cannot reach the
     * worker thread. The SDK's own per-attempt timeout is what actually bounds a stuck send, which is why
     * {@link #warnIfSendTimeoutIsTooTight()} checks the two against each other at startup.
     */
    private static void cancel(List<CompletableFuture<Void>> inFlight) {
        inFlight.forEach(future -> future.cancel(true));
    }

    /**
     * Hands a poison record to the dead letter queue, or fails the task when none is configured.
     *
     * <p>Without a DLQ the old behaviour is the only safe one: dropping the record silently would be data loss the
     * operator never hears about.
     */
    private void reportBadRecord(SinkRecord record, DataException ex) {
        if (errantRecordReporter == null) {
            throw new ConnectException("Unable to convert a record and no dead letter queue is configured "
                    + "(set errors.tolerance=all and errors.deadletterqueue.topic.name to keep the task running) "
                    + "[topic=" + record.topic() + ", partition=" + record.kafkaPartition()
                    + ", offset=" + record.kafkaOffset() + "]", ex);
        }
        log.warn("Dead lettering unconvertible record [topic={}, partition={}, offset={}]: {}",
                record.topic(), record.kafkaPartition(), record.kafkaOffset(), ex.getMessage());
        errantRecordReporter.report(record, ex);
    }

    /**
     * @return the framework's dead letter reporter, or {@code null} when the runtime or the connector config does
     *         not provide one
     */
    private ErrantRecordReporter resolveErrantRecordReporter() {
        try {
            return context == null ? null : context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError ex) {
            // Connect runtimes older than 2.6 do not have the reporter at all.
            log.info("This Connect runtime does not support a dead letter queue for sink connectors");
            return null;
        }
    }

    private void initializeProducers(short clientsPerTask) {
        EventHubProducerProvider provider;
        try {
            provider = getProducerProvider(config);
        } catch (ConfigException ex) {
            throw new ConnectException("[EVENTHUB_CONFIG] Invalid connector configuration: " + ex.getMessage(), ex);
        }
        List<EventHubProducerClient> opened = new ArrayList<>(clientsPerTask);
        try {
            for (short i = 0; i < clientsPerTask; i++) {
                EventHubProducerClient producer = provider.newProducer();
                opened.add(producer);
                if (validateOnStart) {
                    verifyReachable(producer, provider);
                }
            }
        } catch (RuntimeException ex) {
            opened.forEach(EventHubSinkTask::closeQuietly);
            throw ex;
        }
        producers = opened;
        sendExecutor = Executors.newFixedThreadPool(Math.max(1, opened.size()), new SendThreadFactory());
        log.info("Opened {} Event Hubs producer(s)", opened.size());
    }

    /**
     * Proves the producer can actually reach and authenticate against the Event Hub, before the task reports
     * itself as running.
     *
     * <p>Building a v5 producer client opens nothing - the AMQP connection is established lazily on the first
     * send. Without this probe a task configured with a rejected credential or a misspelled Event Hub sits in
     * RUNNING until the first record arrives, which may be hours later and on somebody else's shift.
     *
     * <p>{@code createBatch()} is the probe rather than {@code getEventHubProperties()} because it attaches the
     * send link, which needs only the {@code Send} claim. Reading entity properties needs {@code Listen}, so it
     * would reject the least-privilege policy this connector documents.
     *
     * <p>The two outcomes are deliberately asymmetric:
     * <ul>
     *   <li>a configuration fault fails immediately, with an explanation - retrying a rejected credential only
     *       delays the same failure;</li>
     *   <li>a transient fault is retried until {@code eventhub.start.timeout}, so a worker that restarts during a
     *       brief network blip comes up instead of needing a manual nudge.</li>
     * </ul>
     */
    private void verifyReachable(EventHubProducerClient producer, EventHubProducerProvider provider) {
        String target = "namespace=" + provider.fullyQualifiedNamespace()
                + ", eventHub=" + provider.entityPath()
                + ", auth=" + config.getString(EventHubSinkConfig.AUTHENTICATION_PROVIDER);
        long deadlineNanos = System.nanoTime() + startTimeout.toNanos();
        long backoffMillis = 500L;

        while (true) {
            try {
                producer.createBatch();
                log.info("Verified connectivity to Event Hubs [{}]", target);
                return;
            } catch (RuntimeException ex) {
                String diagnosis = EventHubErrors.diagnose(ex);
                if (diagnosis != null) {
                    throw new ConnectException(diagnosis + " Target [" + target + "], reported by Event Hubs as "
                            + EventHubErrors.describe(ex), ex);
                }
                if (!EventHubErrors.isRetriable(ex)) {
                    throw new ConnectException("[EVENTHUB_UNREACHABLE] Unable to reach Event Hubs. Target ["
                            + target + "]: " + EventHubErrors.describe(ex), ex);
                }
                if (System.nanoTime() >= deadlineNanos) {
                    throw new RetriableException("Event Hubs was still unreachable after "
                            + EventHubSinkConfig.START_TIMEOUT + "=" + startTimeout.getSeconds() + "s ["
                            + target + "]: " + EventHubErrors.describe(ex), ex);
                }
                log.warn("Event Hubs not reachable yet [{}], retrying in {}ms: {}",
                        target, backoffMillis, EventHubErrors.describe(ex));
                sleep(backoffMillis);
                backoffMillis = Math.min(backoffMillis * 2, 5_000L);
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RetriableException("Interrupted while waiting for Event Hubs to become reachable", ex);
        }
    }

    private void shutdownExecutor() {
        if (sendExecutor == null) {
            return;
        }
        sendExecutor.shutdownNow();
        try {
            if (!sendExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Event Hubs send threads did not terminate within 30s");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        sendExecutor = null;
    }

    private static void closeQuietly(EventHubProducerClient producer) {
        try {
            producer.close();
        } catch (RuntimeException ex) {
            log.warn("Error closing a partially initialized Event Hubs producer: {}", EventHubErrors.describe(ex));
        }
    }

    /** One Event Hubs batch, the producer it will be sent on, and the records it carries. */
    private static final class PendingBatch {
        private final EventHubProducerClient producer;
        private final EventDataBatch batch;
        private final String partitionKey;
        private final List<SinkRecord> records = new ArrayList<>();

        private PendingBatch(EventHubProducerClient producer, EventDataBatch batch, String partitionKey) {
            this.producer = producer;
            this.batch = batch;
            this.partitionKey = partitionKey;
        }

        /**
         * Names the Kafka offsets in this batch, so a failure log points at the records an operator has to reason
         * about rather than only at the exception.
         */
        @Override
        public String toString() {
            if (records.isEmpty()) {
                return "batch[empty]";
            }
            SinkRecord first = records.get(0);
            SinkRecord last = records.get(records.size() - 1);
            return "batch[events=" + batch.getCount() + ", bytes=" + batch.getSizeInBytes()
                    + ", partitionKey=" + partitionKey
                    + ", from=" + first.topic() + "-" + first.kafkaPartition() + "@" + first.kafkaOffset()
                    + ", to=" + last.topic() + "-" + last.kafkaPartition() + "@" + last.kafkaOffset() + "]";
        }
    }

    /**
     * Named daemon threads. Connect isolates plugins in their own classloader; non-daemon pool threads that outlive
     * a deleted connector keep that classloader alive and leak it.
     */
    private static final class SendThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "eventhub-sink-send-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
