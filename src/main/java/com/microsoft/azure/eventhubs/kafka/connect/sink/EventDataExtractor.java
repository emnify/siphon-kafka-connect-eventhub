package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.messaging.eventhubs.EventData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Turns a {@link SinkRecord} value into an Event Hubs {@link EventData}.
 *
 * <p>Every failure here is a {@link DataException}, never a {@code ConnectException}. That distinction is what lets
 * the task route a single malformed record to the dead letter queue instead of failing the whole task: a record
 * that cannot be serialized now will not serialize on a retry either, so it must never be treated as retriable.
 */
public class EventDataExtractor {

    private final OutputJsonFormatter valueConvertor = new OutputJsonFormatter();

    public EventDataExtractor() {
    }

    /**
     * @return the event to send, or {@code null} for a tombstone (null-valued) record, which is skipped
     * @throws DataException if the record value cannot be converted; the record is poison and must be dead lettered
     */
    public EventData extractEventData(SinkRecord record) {
        Object value = record.value();
        if (value == null) {
            return null;
        }
        if (value instanceof EventData) {
            return (EventData) value;
        }
        if (value instanceof byte[]) {
            return new EventData((byte[]) value);
        }
        if (value instanceof ByteBuffer) {
            return new EventData((ByteBuffer) value);
        }
        if (value instanceof String) {
            return new EventData((String) value);
        }
        if (value instanceof Struct) {
            return structToEventData(record.topic(), record.valueSchema(), (Struct) value);
        }
        throw new DataException("Unsupported record value type for the Event Hubs sink [class="
                + value.getClass().getCanonicalName() + "]. Supported: Struct, byte[], ByteBuffer, String, EventData.");
    }

    /** Derives the Event Hubs partition key from the record key. Returns {@code null} when there is no usable key. */
    public static String partitionKeyOf(SinkRecord record) {
        Object key = record.key();
        if (key == null) {
            return null;
        }
        if (key instanceof String) {
            return ((String) key).isEmpty() ? null : (String) key;
        }
        if (key instanceof byte[]) {
            byte[] bytes = (byte[]) key;
            return bytes.length == 0 ? null : new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        return key.toString();
    }

    public void configureJson(Map<String, ?> jsonConfigMap) {
        valueConvertor.configure(jsonConfigMap);
    }

    private EventData structToEventData(String topic, Schema schema, Struct struct) {
        try {
            byte[] bytes = valueConvertor.fromConnectData(topic, schema, struct);
            if (bytes == null) {
                return null;
            }
            EventData eventData = new EventData(bytes);
            eventData.setContentType("application/json");
            return eventData;
        } catch (DataException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            // Deliberately does not include struct.toString(): record values routinely carry personal data, and
            // this message goes to the connector log and the dead letter header.
            throw new DataException("Unable to convert record to JSON [topic=" + topic
                    + ", schema=" + (schema == null ? "none" : schema.name()) + "]", ex);
        }
    }
}
