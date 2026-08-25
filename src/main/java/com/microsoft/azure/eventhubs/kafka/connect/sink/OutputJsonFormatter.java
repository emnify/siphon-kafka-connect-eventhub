package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.StringConverterConfig;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Converts a Connect {@link Struct} into the JSON body of an Event Hubs event.
 *
 * <p>This is a trimmed relative of Connect's own {@code JsonConverter}: it emits the payload only, never the
 * {@code {"schema":..,"payload":..}} envelope, and it can omit null fields entirely.
 *
 * <p>Formatting state is per instance. It used to be {@code static}, which meant a second connector in the same
 * worker would have quietly shared the first one's date formats.
 */
public class OutputJsonFormatter {

    private final OutputJsonSerializer serializer = new OutputJsonSerializer();

    private DateTimeFormatter dateTimeFormat = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private DateTimeFormatter dateFormat = DateTimeFormatter.ISO_LOCAL_DATE;
    private DateTimeFormatter timeFormat = DateTimeFormatter.ISO_LOCAL_TIME;
    private ZoneId timestampZone = ZoneOffset.UTC;
    private boolean skipNulls = OutputJsonFormatterConfig.NULL_DEFAULT;

    public OutputJsonFormatter() {
    }

    public void configure(Map<String, ?> configs) {
        Map<String, Object> conf = new HashMap<>(configs);
        conf.put(StringConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        OutputJsonFormatterConfig config = new OutputJsonFormatterConfig(conf);
        skipNulls = config.enableSkipNulls();
        dateTimeFormat = config.dateTimeFormat();
        dateFormat = config.dateFormat();
        timeFormat = config.timeFormat();
        timestampZone = config.timestampZone();
        serializer.configure(conf, false);
    }

    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema == null && value == null) {
            return null;
        }
        JsonNode jsonValue = convertToJson(schema, value);
        try {
            return serializer.serialize(topic, jsonValue);
        } catch (SerializationException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    /**
     * Converts a value in the {@code org.apache.kafka.connect.data} format into a Jackson tree.
     *
     * <p>Logical types are resolved before the switch on schema type, because a Timestamp is an INT64 on the wire
     * but must not be emitted as a number.
     */
    private JsonNode convertToJson(Schema schema, Object logicalValue) {
        if (logicalValue == null) {
            if (schema == null) {
                // Any schema is valid and there is no default, so treat this as an optional schema.
                return null;
            }
            if (schema.defaultValue() != null) {
                return convertToJson(schema, schema.defaultValue());
            }
            if (schema.isOptional()) {
                return JsonNodeFactory.instance.nullNode();
            }
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        if (schema != null && schema.name() != null) {
            JsonNode logical = convertLogicalType(schema, logicalValue);
            if (logical != null) {
                return logical;
            }
        }

        Object value = logicalValue;
        try {
            final Schema.Type schemaType;
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(value.getClass());
                if (schemaType == null) {
                    throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
                }
            } else {
                schemaType = schema.type();
            }
            switch (schemaType) {
                case INT8:
                    return JsonNodeFactory.instance.numberNode((Byte) value);
                case INT16:
                    return JsonNodeFactory.instance.numberNode((Short) value);
                case INT32:
                    return JsonNodeFactory.instance.numberNode((Integer) value);
                case INT64:
                    return JsonNodeFactory.instance.numberNode((Long) value);
                case FLOAT32:
                    return JsonNodeFactory.instance.numberNode((Float) value);
                case FLOAT64:
                    return JsonNodeFactory.instance.numberNode((Double) value);
                case BOOLEAN:
                    return JsonNodeFactory.instance.booleanNode((Boolean) value);
                case STRING:
                    return JsonNodeFactory.instance.textNode(((CharSequence) value).toString());
                case BYTES:
                    if (value instanceof byte[]) {
                        return JsonNodeFactory.instance.binaryNode((byte[]) value);
                    } else if (value instanceof ByteBuffer) {
                        return JsonNodeFactory.instance.binaryNode(((ByteBuffer) value).array());
                    }
                    throw new DataException("Invalid type for bytes type: " + value.getClass());
                case ARRAY: {
                    Collection<?> collection = (Collection<?>) value;
                    ArrayNode list = JsonNodeFactory.instance.arrayNode();
                    Schema valueSchema = schema == null ? null : schema.valueSchema();
                    for (Object elem : collection) {
                        // Nulls inside an array are kept even when skipNulls is on: dropping them would shift every
                        // following element's index.
                        list.add(convertToJson(valueSchema, elem));
                    }
                    return list;
                }
                case MAP:
                    return convertMap(schema, (Map<?, ?>) value);
                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (!struct.schema().equals(schema)) {
                        throw new DataException("Mismatching schema.");
                    }
                    ObjectNode obj = JsonNodeFactory.instance.objectNode();
                    for (Field field : schema.fields()) {
                        Object fieldValue = struct.get(field);
                        if (!skipNulls || fieldValue != null) {
                            obj.set(field.name(), convertToJson(field.schema(), fieldValue));
                        }
                    }
                    return obj;
                }
                default:
                    throw new DataException("Couldn't convert " + value + " to JSON.");
            }
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
        }
    }

    private JsonNode convertMap(Schema schema, Map<?, ?> map) {
        // With string keys the map becomes a JSON object; otherwise it becomes an array of [key, value] pairs,
        // because JSON object keys can only be strings.
        boolean objectMode;
        if (schema == null) {
            objectMode = map.keySet().stream().allMatch(key -> key instanceof String);
        } else {
            objectMode = schema.keySchema().type() == Schema.Type.STRING;
        }

        Schema keySchema = schema == null ? null : schema.keySchema();
        Schema valueSchema = schema == null ? null : schema.valueSchema();
        ObjectNode obj = objectMode ? JsonNodeFactory.instance.objectNode() : null;
        ArrayNode list = objectMode ? null : JsonNodeFactory.instance.arrayNode();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            JsonNode mapKey = convertToJson(keySchema, entry.getKey());
            JsonNode mapValue = convertToJson(valueSchema, entry.getValue());
            if (skipNulls && (mapValue == null || mapValue.isNull())) {
                continue;
            }
            if (objectMode) {
                obj.set(mapKey.asText(), mapValue);
            } else {
                list.add(JsonNodeFactory.instance.arrayNode().add(mapKey).add(mapValue));
            }
        }
        return objectMode ? obj : list;
    }

    /**
     * @return the JSON form of a Connect logical type, or {@code null} if the schema does not name one
     */
    private JsonNode convertLogicalType(Schema schema, Object value) {
        String name = schema.name();
        if (Decimal.LOGICAL_NAME.equals(name)) {
            if (!(value instanceof BigDecimal)) {
                throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
            }
            return JsonNodeFactory.instance.binaryNode(Decimal.fromLogical(schema, (BigDecimal) value));
        }
        if (Date.LOGICAL_NAME.equals(name)) {
            requireDate(name, value);
            LocalDate date = LocalDate.ofEpochDay(Date.fromLogical(schema, (java.util.Date) value));
            return JsonNodeFactory.instance.textNode(dateFormat.format(date));
        }
        if (Time.LOGICAL_NAME.equals(name)) {
            requireDate(name, value);
            // Time.fromLogical yields milliseconds since midnight, which is a time of day - not an epoch instant.
            // Feeding it to Instant.ofEpochMilli and then LocalDate.from threw DateTimeException on every record.
            LocalTime time = LocalTime.ofNanoOfDay(Time.fromLogical(schema, (java.util.Date) value) * 1_000_000L);
            return JsonNodeFactory.instance.textNode(timeFormat.format(time));
        }
        if (Timestamp.LOGICAL_NAME.equals(name)) {
            requireDate(name, value);
            Instant instant = Instant.ofEpochMilli(Timestamp.fromLogical(schema, (java.util.Date) value));
            return JsonNodeFactory.instance.textNode(dateTimeFormat.format(LocalDateTime.ofInstant(instant, timestampZone)));
        }
        return null;
    }

    private static void requireDate(String logicalName, Object value) {
        if (!(value instanceof java.util.Date)) {
            throw new DataException("Invalid type for " + logicalName + ", expected Date but was " + value.getClass());
        }
    }
}
