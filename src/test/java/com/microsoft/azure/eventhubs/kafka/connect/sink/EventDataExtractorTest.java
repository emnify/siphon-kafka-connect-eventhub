package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.messaging.eventhubs.EventData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class EventDataExtractorTest {

    private final EventDataExtractor extractor = new EventDataExtractor();

    private static SinkRecord record(Object value) {
        return new SinkRecord("topic", 0, null, null, null, value, 0);
    }

    @Test
    public void bytesArePassedThroughUnchanged() {
        EventData event = extractor.extractEventData(record("hello".getBytes(StandardCharsets.UTF_8)));
        assertEquals("hello", event.getBodyAsString());
    }

    @Test
    public void byteBuffersAreSupported() {
        EventData event = extractor.extractEventData(
                record(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))));
        assertEquals("hello", event.getBodyAsString());
    }

    @Test
    public void stringsAreSupported() {
        assertEquals("hello", extractor.extractEventData(record("hello")).getBodyAsString());
    }

    @Test
    public void anEventDataValueIsForwardedAsIs() {
        EventData original = new EventData("hello");
        assertEquals(original, extractor.extractEventData(record(original)));
    }

    @Test
    public void tombstonesProduceNoEvent() {
        assertNull(extractor.extractEventData(record(null)));
    }

    @Test
    public void structsBecomeJsonWithAContentType() {
        Schema schema = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).build();
        EventData event = extractor.extractEventData(
                new SinkRecord("topic", 0, null, null, schema, new Struct(schema).put("a", "b"), 0));

        assertEquals("{\"a\":\"b\"}", event.getBodyAsString());
        assertEquals("application/json", event.getContentType());
    }

    @Test
    public void unsupportedValueTypesRaiseDataExceptionSoTheyCanBeDeadLettered() {
        // Must not be a ConnectException: that would be treated as a task-level failure rather than a poison record.
        DataException ex = assertThrows(DataException.class, () -> extractor.extractEventData(record(42)));
        assertTrue(ex.getMessage(), ex.getMessage().contains("Integer"));
    }

    @Test
    public void conversionFailuresDoNotEchoTheRecordValue() {
        // Record values routinely carry personal data, and this message reaches the log and the dead letter header.
        java.util.Map<String, Object> config = new java.util.HashMap<>();
        config.put(OutputJsonFormatterConfig.NULL_CONFIG, "false");
        extractor.configureJson(config);

        Schema schema = SchemaBuilder.struct()
                .field("secret", Schema.STRING_SCHEMA)
                .field("required", Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(schema).put("secret", "national-insurance-number");

        DataException ex = assertThrows(DataException.class,
                () -> extractor.extractEventData(new SinkRecord("topic", 0, null, null, schema, value, 0)));
        assertEquals(-1, ex.getMessage().indexOf("national-insurance-number"));
        assertEquals(-1, ex.getMessage().indexOf("Struct{"));
    }

    @Test
    public void partitionKeysAreDerivedFromCommonKeyTypes() {
        assertEquals("k", EventDataExtractor.partitionKeyOf(
                new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "k", null, null, 0)));
        assertEquals("k", EventDataExtractor.partitionKeyOf(
                new SinkRecord("topic", 0, Schema.BYTES_SCHEMA, "k".getBytes(StandardCharsets.UTF_8), null, null, 0)));
        assertEquals("7", EventDataExtractor.partitionKeyOf(
                new SinkRecord("topic", 0, Schema.INT32_SCHEMA, 7, null, null, 0)));
    }

    @Test
    public void absentOrEmptyKeysYieldNoPartitionKey() {
        // An empty partition key is rejected by Event Hubs, so it has to become "no key" rather than "".
        assertNull(EventDataExtractor.partitionKeyOf(record(null)));
        assertNull(EventDataExtractor.partitionKeyOf(
                new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "", null, null, 0)));
        assertNull(EventDataExtractor.partitionKeyOf(
                new SinkRecord("topic", 0, Schema.BYTES_SCHEMA, new byte[0], null, null, 0)));
    }
}
