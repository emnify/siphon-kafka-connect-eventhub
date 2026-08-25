package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.core.amqp.exception.AmqpErrorCondition;
import com.azure.core.amqp.exception.AmqpErrorContext;
import com.azure.core.amqp.exception.AmqpException;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventHubSinkTaskTest {

    private static final String CONNECTION_STRING =
            "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=k;EntityPath=hub";
    private static final AmqpErrorContext AMQP_CONTEXT = new AmqpErrorContext("ns.servicebus.windows.net");

    private TestableTask task;
    private List<EventHubProducerClient> producers;
    private List<CreateBatchOptions> createdBatchOptions;
    /** Number of events a mocked batch accepts before reporting itself full; -1 means unlimited. */
    private int batchCapacity = -1;
    /** Thrown by createBatch() until it has been called this many times; simulates a Event Hub that is not ready. */
    private int failCreateBatchTimes;
    private RuntimeException createBatchFailure;
    private final AtomicInteger createBatchCalls = new AtomicInteger();

    @Before
    public void setUp() {
        producers = new ArrayList<>();
        createdBatchOptions = new ArrayList<>();
        createBatchCalls.set(0);
        failCreateBatchTimes = 0;
        createBatchFailure = null;
        task = new TestableTask();
    }

    @After
    public void tearDown() {
        task.stop();
    }

    @Test
    public void opensOneProducerPerConfiguredClient() {
        start(3, null);
        assertEquals(3, task.getClientCount());
    }

    @Test
    public void packsAllRecordsOfABatchIntoASingleSend() {
        // The v3 implementation issued one AMQP send per record; batching is the whole point of the rewrite.
        start(1, null);
        task.put(byteRecords(20));

        verify(producers.get(0), times(1)).send(any(EventDataBatch.class));
    }

    @Test
    public void sealsAFullBatchAndStartsANewOne() {
        batchCapacity = 8;
        start(1, null);
        task.put(byteRecords(20));

        verify(producers.get(0), times(3)).send(any(EventDataBatch.class)); // 8 + 8 + 4
    }

    @Test
    public void spreadsBatchesAcrossProducers() {
        batchCapacity = 5;
        start(2, null);
        task.put(byteRecords(10));

        verify(producers.get(0), atLeastOnce()).send(any(EventDataBatch.class));
        verify(producers.get(1), atLeastOnce()).send(any(EventDataBatch.class));
    }

    @Test
    public void structRecordsAreSerializedToJson() {
        start(1, null);
        task.put(structRecords(5));

        verify(producers.get(0), times(1)).send(any(EventDataBatch.class));
    }

    @Test
    public void tombstonesAreSkipped() {
        start(1, null);
        task.put(Collections.singletonList(
                new SinkRecord("topic", 0, null, null, null, null, 0)));

        verify(producers.get(0), never()).send(any(EventDataBatch.class));
    }

    @Test
    public void emptyPutDoesNothing() {
        start(1, null);
        task.put(Collections.emptyList());

        verify(producers.get(0), never()).send(any(EventDataBatch.class));
    }

    @Test
    public void noPartitionKeyIsSetByDefault() {
        start(1, null);
        task.put(keyedRecords("tenant-a", "tenant-b"));

        assertEquals(1, createdBatchOptions.size());
        assertEquals(null, createdBatchOptions.get(0).getPartitionKey());
    }

    @Test
    public void recordKeysBecomePartitionKeysWhenConfigured() {
        Map<String, String> extra = new HashMap<>();
        extra.put(EventHubSinkConfig.PARTITION_KEY_SOURCE, EventHubSinkConfig.PARTITION_KEY_RECORD_KEY);
        start(1, null, extra);
        task.put(keyedRecords("tenant-a", "tenant-b", "tenant-a"));

        // One batch per distinct key, so records sharing a key stay in one Event Hubs partition and keep their order.
        List<String> keys = new ArrayList<>();
        createdBatchOptions.forEach(options -> keys.add(options.getPartitionKey()));
        assertEquals(2, keys.size());
        assertTrue(keys.toString(), keys.contains("tenant-a") && keys.contains("tenant-b"));
        verify(producers.get(0), times(2)).send(any(EventDataBatch.class));
    }

    @Test
    public void transientSendFailuresAskConnectToRetryTheBatch() {
        start(1, null);
        doThrow(new AmqpException(true, AmqpErrorCondition.SERVER_BUSY_ERROR, "throttled", AMQP_CONTEXT))
                .when(producers.get(0)).send(any(EventDataBatch.class));

        RetriableException ex = assertThrows(RetriableException.class, () -> task.put(byteRecords(5)));
        assertTrue(ex.getMessage(), ex.getMessage().contains("SERVER_BUSY_ERROR"));
    }

    @Test
    public void failureMessagesNameTheAffectedKafkaOffsets() {
        start(1, null);
        doThrow(new AmqpException(true, AmqpErrorCondition.LINK_DETACH_FORCED, "detached", AMQP_CONTEXT))
                .when(producers.get(0)).send(any(EventDataBatch.class));

        RetriableException ex = assertThrows(RetriableException.class, () -> task.put(byteRecords(5)));
        assertTrue(ex.getMessage(), ex.getMessage().contains("topic-0@0"));
    }

    @Test
    public void permanentSendFailuresStopTheTaskInsteadOfSpinning() {
        start(1, null);
        doThrow(new AmqpException(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS, "denied", AMQP_CONTEXT))
                .when(producers.get(0)).send(any(EventDataBatch.class));

        ConnectException ex = assertThrows(ConnectException.class, () -> task.put(byteRecords(5)));
        // Any ConnectException that is not a RetriableException stops the task; the credential case is reported
        // through a plain ConnectException carrying a tokenised, actionable message.
        assertTrue(ex.getClass().getName(), !(ex instanceof RetriableException));
        assertEquals(ConnectException.class, ex.getClass());
    }

    @Test
    public void aPermanentFailureWinsOverAConcurrentTransientOne() {
        // Otherwise one poison batch hides behind a throttling response and retries until errors.retry.timeout.
        batchCapacity = 5;
        start(2, null);
        doThrow(new AmqpException(true, AmqpErrorCondition.SERVER_BUSY_ERROR, "throttled", AMQP_CONTEXT))
                .when(producers.get(0)).send(any(EventDataBatch.class));
        doThrow(new AmqpException(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS, "denied", AMQP_CONTEXT))
                .when(producers.get(1)).send(any(EventDataBatch.class));

        ConnectException ex = assertThrows(ConnectException.class, () -> task.put(byteRecords(10)));
        assertTrue(ex.getClass().getName(), !(ex instanceof RetriableException));
    }

    @Test
    public void unconvertibleRecordsFailTheTaskWhenNoDeadLetterQueueIsConfigured() {
        start(1, null);
        // Silently dropping the record would be data loss the operator never hears about.
        assertThrows(ConnectException.class, () -> task.put(unsupportedValueRecords(1)));
    }

    @Test
    public void unconvertibleRecordsAreDeadLetteredWhenAReporterIsAvailable() {
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        start(1, reporter);
        task.put(unsupportedValueRecords(1));

        verify(reporter, times(1)).report(any(SinkRecord.class), any(Throwable.class));
        verify(producers.get(0), never()).send(any(EventDataBatch.class));
    }

    @Test
    public void goodRecordsStillShipWhenOneRecordIsDeadLettered() {
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        start(1, reporter);

        List<SinkRecord> records = new ArrayList<>(byteRecords(3));
        records.addAll(unsupportedValueRecords(1));
        task.put(records);

        verify(reporter, times(1)).report(any(SinkRecord.class), any(Throwable.class));
        verify(producers.get(0), times(1)).send(any(EventDataBatch.class));
    }

    @Test
    public void aRecordTooLargeForAnEmptyBatchIsDeadLetteredRatherThanRetriedForever() {
        batchCapacity = 0; // every tryAdd fails, on an empty batch
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        start(1, reporter);
        task.put(byteRecords(2));

        verify(reporter, times(2)).report(any(SinkRecord.class), any(Throwable.class));
        verify(producers.get(0), never()).send(any(EventDataBatch.class));
    }

    @Test
    public void stopClosesEveryProducer() {
        start(2, null);
        task.stop();

        producers.forEach(producer -> verify(producer, times(1)).close());
    }

    @Test
    public void stopBeforeStartIsSafe() {
        new EventHubSinkTask().stop();
    }

    @Test
    public void versionIsReported() {
        assertTrue(new EventHubSinkTask().version() != null);
    }


    // --- startup validation -------------------------------------------------------------------------------

    @Test
    public void startupProvesTheConnectionBeforeReportingRunning() {
        // buildProducerClient() opens nothing, so without an explicit probe a misconfigured task sits in RUNNING
        // until the first record arrives.
        start(1, null);
        verify(producers.get(0), times(1)).createBatch();
    }

    @Test
    public void rejectedCredentialsFailFastWithAnActionableMessage() {
        failCreateBatchTimes = Integer.MAX_VALUE;
        createBatchFailure = new AmqpException(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS, "denied", AMQP_CONTEXT);

        ConnectException ex =
                assertThrows(ConnectException.class, () -> start(1, null));

        assertTrue(ex.getMessage(), ex.getMessage().contains("EVENTHUB_UNAUTHORIZED"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("'Send' claim"));
        assertTrue(ex.getMessage(), ex.getMessage().contains(EventHubSinkConfig.CONNECTION_STRING));
        assertTrue(ex.getMessage(), ex.getMessage().contains("ns.servicebus.windows.net"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("UNAUTHORIZED_ACCESS"));
    }

    @Test
    public void aMissingEventHubFailsFast() {
        failCreateBatchTimes = Integer.MAX_VALUE;
        createBatchFailure = new AmqpException(false, AmqpErrorCondition.NOT_FOUND, "no such entity", AMQP_CONTEXT);

        ConnectException ex =
                assertThrows(ConnectException.class, () -> start(1, null));
        assertTrue(ex.getMessage(), ex.getMessage().contains("EVENTHUB_NOT_FOUND"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("EntityPath"));
    }

    @Test
    public void aDisabledEventHubFailsFast() {
        failCreateBatchTimes = Integer.MAX_VALUE;
        createBatchFailure =
                new AmqpException(false, AmqpErrorCondition.ENTITY_DISABLED_ERROR, "disabled", AMQP_CONTEXT);

        assertTrue(assertThrows(ConnectException.class, () -> start(1, null))
                .getMessage().contains("disabled"));
    }

    @Test
    public void configurationFailuresAreNotRetriable() {
        // A RetriableException here would burn the whole errors.retry.timeout window before failing anyway.
        failCreateBatchTimes = Integer.MAX_VALUE;
        createBatchFailure = new AmqpException(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS, "denied", AMQP_CONTEXT);

        Throwable ex = assertThrows(ConnectException.class, () -> start(1, null));
        assertTrue(!(ex instanceof RetriableException));
    }

    @Test
    public void aBriefOutageAtStartupIsRetriedRatherThanFailed() {
        failCreateBatchTimes = 2;
        createBatchFailure =
                new AmqpException(true, AmqpErrorCondition.CONNECTION_FORCED, "recycling", AMQP_CONTEXT);

        start(1, null);

        assertEquals(1, task.getClientCount());
        assertEquals(3, createBatchCalls.get()); // two failures, then success
    }

    @Test
    public void aSustainedOutageAtStartupGivesUpAfterTheStartTimeout() {
        failCreateBatchTimes = Integer.MAX_VALUE;
        createBatchFailure = new AmqpException(true, AmqpErrorCondition.SERVER_BUSY_ERROR, "busy", AMQP_CONTEXT);
        Map<String, String> extra = new HashMap<>();
        extra.put(EventHubSinkConfig.START_TIMEOUT, "0");

        RetriableException ex = assertThrows(RetriableException.class, () -> start(1, null, extra));
        assertTrue(ex.getMessage(), ex.getMessage().contains(EventHubSinkConfig.START_TIMEOUT));
    }

    @Test
    public void aFailedStartupClosesTheProducersItOpened() {
        failCreateBatchTimes = Integer.MAX_VALUE;
        createBatchFailure = new AmqpException(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS, "denied", AMQP_CONTEXT);

        assertThrows(ConnectException.class, () -> start(2, null));

        assertEquals(1, producers.size()); // the second producer is never opened
        verify(producers.get(0), times(1)).close();
    }

    @Test
    public void startupValidationCanBeDisabled() {
        failCreateBatchTimes = Integer.MAX_VALUE;
        createBatchFailure = new AmqpException(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS, "denied", AMQP_CONTEXT);
        Map<String, String> extra = new HashMap<>();
        extra.put(EventHubSinkConfig.VALIDATE_ON_START, "false");

        start(1, null, extra);
        assertEquals(1, task.getClientCount());
    }

    @Test
    public void configurationFailuresOnTheSendPathAreAlsoReportedAsSuch() {
        start(1, null);
        doThrow(new AmqpException(true, AmqpErrorCondition.UNAUTHORIZED_ACCESS, "denied", AMQP_CONTEXT))
                .when(producers.get(0)).send(any(EventDataBatch.class));

        ConnectException ex =
                assertThrows(ConnectException.class, () -> task.put(byteRecords(5)));
        assertTrue(ex.getMessage(), ex.getMessage().contains("EVENTHUB_UNAUTHORIZED"));
    }

    private void start(int clientsPerTask, ErrantRecordReporter reporter) {
        start(clientsPerTask, reporter, Collections.emptyMap());
    }

    private void start(int clientsPerTask, ErrantRecordReporter reporter, Map<String, String> extra) {
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.errantRecordReporter()).thenReturn(reporter);
        task.initialize(context);

        Map<String, String> props = new HashMap<>(extra);
        props.putIfAbsent(EventHubSinkConfig.START_TIMEOUT, "10");
        props.put(EventHubSinkConfig.CONNECTION_STRING, CONNECTION_STRING);
        props.put(EventHubSinkConfig.CLIENTS_PER_TASK, String.valueOf(clientsPerTask));
        task.start(props);
    }

    private static List<SinkRecord> byteRecords(int count) {
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            records.add(new SinkRecord("topic", 0, null, null, null, ("payload-" + i).getBytes(), i));
        }
        return records;
    }

    private static List<SinkRecord> keyedRecords(String... keys) {
        List<SinkRecord> records = new ArrayList<>(keys.length);
        for (int i = 0; i < keys.length; i++) {
            records.add(new SinkRecord("topic", 0, Schema.STRING_SCHEMA, keys[i], null,
                    ("payload-" + i).getBytes(), i));
        }
        return records;
    }

    private static List<SinkRecord> structRecords(int count) {
        Schema schema = SchemaBuilder.struct()
                .name("test.Record")
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Struct value = new Struct(schema).put("field1", "value-" + i).put("field2", i);
            records.add(new SinkRecord("topic", 0, null, null, schema, value, i));
        }
        return records;
    }

    private static List<SinkRecord> unsupportedValueRecords(int count) {
        List<SinkRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            records.add(new SinkRecord("topic", 0, null, null, null, new java.util.concurrent.atomic.AtomicLong(i), i));
        }
        return records;
    }

    /** A task wired to mocked producers, so nothing in this suite reaches Azure. */
    private final class TestableTask extends EventHubSinkTask {
        @Override
        protected EventHubProducerProvider getProducerProvider(EventHubSinkConfig config) {
            EventHubProducerProvider provider = mock(EventHubProducerProvider.class);
            when(provider.newProducer()).thenAnswer(invocation -> newMockProducer());
            when(provider.fullyQualifiedNamespace()).thenReturn("ns.servicebus.windows.net");
            when(provider.entityPath()).thenReturn("hub");
            return provider;
        }
    }

    private EventHubProducerClient newMockProducer() {
        EventHubProducerClient producer = mock(EventHubProducerClient.class);
        when(producer.createBatch()).thenAnswer(invocation -> {
            if (createBatchCalls.incrementAndGet() <= failCreateBatchTimes) {
                throw createBatchFailure;
            }
            return newMockBatch();
        });
        when(producer.createBatch(any(CreateBatchOptions.class))).thenAnswer(invocation -> {
            createdBatchOptions.add(invocation.getArgument(0));
            return newMockBatch();
        });
        producers.add(producer);
        return producer;
    }

    private EventDataBatch newMockBatch() {
        EventDataBatch batch = mock(EventDataBatch.class);
        AtomicInteger count = new AtomicInteger();
        when(batch.tryAdd(any(EventData.class))).thenAnswer(invocation -> {
            if (batchCapacity >= 0 && count.get() >= batchCapacity) {
                return false;
            }
            count.incrementAndGet();
            return true;
        });
        when(batch.getCount()).thenAnswer(invocation -> count.get());
        when(batch.getMaxSizeInBytes()).thenReturn(1024 * 1024);
        when(batch.getSizeInBytes()).thenAnswer(invocation -> count.get() * 16);
        return batch;
    }
}
