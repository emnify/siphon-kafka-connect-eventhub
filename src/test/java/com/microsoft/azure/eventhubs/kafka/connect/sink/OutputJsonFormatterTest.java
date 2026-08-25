package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class OutputJsonFormatterTest {

    private static final long EPOCH_MILLIS = 1591277777159L; // 2020-06-04T13:36:17.159Z

    private static String json(OutputJsonFormatter formatter, Schema schema, Struct value) {
        return new String(formatter.fromConnectData("topic", schema, value), StandardCharsets.UTF_8);
    }

    @Test
    public void serializesAStructAndOmitsNullsByDefault() {
        Schema schema = SchemaBuilder.struct()
                .field("textField", Schema.STRING_SCHEMA)
                .field("fieldNull", Schema.OPTIONAL_STRING_SCHEMA)
                .field("boolField", Schema.BOOLEAN_SCHEMA)
                .field("timestampField", Timestamp.SCHEMA)
                .build();
        Struct value = new Struct(schema)
                .put("textField", "test")
                .put("boolField", false)
                .put("timestampField", new java.util.Date(EPOCH_MILLIS));

        assertEquals("{\"textField\":\"test\",\"boolField\":false,\"timestampField\":\"2020-06-04T13:36:17.159\"}",
                json(new OutputJsonFormatter(), schema, value));
    }

    @Test
    public void nullsAreEmittedWhenSkipNullsIsDisabled() {
        Schema schema = SchemaBuilder.struct()
                .field("present", Schema.STRING_SCHEMA)
                .field("absent", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct value = new Struct(schema).put("present", "x");

        OutputJsonFormatter formatter = new OutputJsonFormatter();
        Map<String, Object> config = new HashMap<>();
        config.put(OutputJsonFormatterConfig.NULL_CONFIG, "false");
        formatter.configure(config);

        assertEquals("{\"present\":\"x\",\"absent\":null}", json(formatter, schema, value));
    }

    @Test
    public void timeLogicalTypeSerializesAsATimeOfDay() {
        // Regression: Time.fromLogical yields millis since midnight, which the old converter fed to
        // Instant.ofEpochMilli and then LocalDate.from - throwing DateTimeException on every record with a Time.
        Schema schema = SchemaBuilder.struct().field("t", Time.SCHEMA).build();
        Struct value = new Struct(schema).put("t", new java.util.Date(3_723_500L)); // 01:02:03.500

        // ISO_LOCAL_TIME emits the minimum fractional digits, so 500ms renders as ".5". Configure
        // json.time.pattern when a downstream consumer needs fixed precision.
        assertEquals("{\"t\":\"01:02:03.5\"}", json(new OutputJsonFormatter(), schema, value));
    }

    @Test
    public void dateLogicalTypeSerializesAsAnIsoDate() {
        Schema schema = SchemaBuilder.struct().field("d", Date.SCHEMA).build();
        Struct value = new Struct(schema).put("d", Date.toLogical(Date.SCHEMA, 18417)); // 2020-06-04

        assertEquals("{\"d\":\"2020-06-04\"}", json(new OutputJsonFormatter(), schema, value));
    }

    @Test
    public void configuredDatePatternsAreApplied() {
        // Regression: json.date.pattern and json.datetime.pattern were defined but never read, so a connector
        // configured with them silently emitted the defaults.
        Schema schema = SchemaBuilder.struct()
                .field("d", Date.SCHEMA)
                .field("ts", Timestamp.SCHEMA)
                .field("t", Time.SCHEMA)
                .build();
        Struct value = new Struct(schema)
                .put("d", Date.toLogical(Date.SCHEMA, 18417))
                .put("ts", new java.util.Date(EPOCH_MILLIS))
                .put("t", new java.util.Date(3_723_000L));

        OutputJsonFormatter formatter = new OutputJsonFormatter();
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(OutputJsonFormatterConfig.FORMATS_DATE_CONFIG, "dd/MM/yyyy");
        config.put(OutputJsonFormatterConfig.FORMATS_DATE_TIME_CONFIG, "yyyy-MM-dd HH:mm:ss");
        config.put(OutputJsonFormatterConfig.FORMATS_TIME_CONFIG, "HH:mm:ss");
        formatter.configure(config);

        assertEquals("{\"d\":\"04/06/2020\",\"ts\":\"2020-06-04 13:36:17\",\"t\":\"01:02:03\"}",
                json(formatter, schema, value));
    }

    @Test
    public void timestampZoneIsConfigurable() {
        Schema schema = SchemaBuilder.struct().field("ts", Timestamp.SCHEMA).build();
        Struct value = new Struct(schema).put("ts", new java.util.Date(EPOCH_MILLIS));

        OutputJsonFormatter formatter = new OutputJsonFormatter();
        Map<String, Object> config = new HashMap<>();
        config.put(OutputJsonFormatterConfig.TIME_ZONE_CONFIG, "Europe/Berlin");
        formatter.configure(config);

        assertEquals("{\"ts\":\"2020-06-04T15:36:17.159\"}", json(formatter, schema, value));
    }

    @Test
    public void invalidPatternIsRejectedAtConfigureTime() {
        OutputJsonFormatter formatter = new OutputJsonFormatter();
        Map<String, Object> config = new HashMap<>();
        config.put(OutputJsonFormatterConfig.FORMATS_DATE_CONFIG, "yyyy-QQQQQQQ");
        assertThrows(ConfigException.class, () -> formatter.configure(config));
    }

    @Test
    public void invalidZoneIsRejectedAtConfigureTime() {
        OutputJsonFormatter formatter = new OutputJsonFormatter();
        Map<String, Object> config = new HashMap<>();
        config.put(OutputJsonFormatterConfig.TIME_ZONE_CONFIG, "Mars/Olympus_Mons");
        assertThrows(ConfigException.class, () -> formatter.configure(config));
    }

    @Test
    public void nestedStructsArraysAndMapsAreSerialized() {
        Schema child = SchemaBuilder.struct().name("child").field("n", Schema.INT32_SCHEMA).build();
        Schema schema = SchemaBuilder.struct()
                .field("child", child)
                .field("list", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
                .build();
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        Struct value = new Struct(schema)
                .put("child", new Struct(child).put("n", 5))
                .put("list", java.util.Arrays.asList("x", "y"))
                .put("map", map);

        assertEquals("{\"child\":{\"n\":5},\"list\":[\"x\",\"y\"],\"map\":{\"a\":1}}",
                json(new OutputJsonFormatter(), schema, value));
    }

    @Test
    public void nullsInsideArraysAreKept() {
        // Dropping them would silently shift the index of every following element.
        Schema schema = SchemaBuilder.struct()
                .field("list", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build())
                .build();
        Struct value = new Struct(schema).put("list", java.util.Arrays.asList("x", null, "y"));

        assertEquals("{\"list\":[\"x\",null,\"y\"]}", json(new OutputJsonFormatter(), schema, value));
    }

    @Test
    public void decimalIsSerializedAsBase64UnscaledBytes() {
        Schema decimalSchema = Decimal.schema(2);
        Schema schema = SchemaBuilder.struct().field("amount", decimalSchema).build();
        Struct value = new Struct(schema).put("amount", new BigDecimal("12.34"));

        assertEquals("{\"amount\":\"BNI=\"}", json(new OutputJsonFormatter(), schema, value));
    }

    @Test
    public void decimalIsSerializedAsANumberWhenNumericFormatIsSelected() {
        Schema schema = SchemaBuilder.struct().field("amount", Decimal.schema(2)).build();
        Struct value = new Struct(schema).put("amount", new BigDecimal("12.34"));

        assertEquals("{\"amount\":12.34}", json(decimalFormatter("NUMERIC"), schema, value));
    }

    @Test
    public void decimalFormatIsCaseInsensitive() {
        Schema schema = SchemaBuilder.struct().field("amount", Decimal.schema(2)).build();
        Struct value = new Struct(schema).put("amount", new BigDecimal("12.34"));

        assertEquals("{\"amount\":12.34}", json(decimalFormatter("numeric"), schema, value));
    }

    @Test
    public void numericDecimalKeepsPlainNotationForANegativeScale() {
        // BigDecimal.toString would render this as 1E+2, which is valid JSON but not what a consumer expects.
        Schema schema = SchemaBuilder.struct().field("amount", Decimal.schema(-2)).build();
        Struct value = new Struct(schema).put("amount", new BigDecimal("1E+2"));

        assertEquals("{\"amount\":100}", json(decimalFormatter("NUMERIC"), schema, value));
    }

    @Test
    public void numericDecimalPreservesTrailingZeroesFromTheSchemaScale() {
        Schema schema = SchemaBuilder.struct().field("amount", Decimal.schema(4)).build();
        Struct value = new Struct(schema).put("amount", new BigDecimal("12.3400"));

        assertEquals("{\"amount\":12.3400}", json(decimalFormatter("NUMERIC"), schema, value));
    }

    @Test
    public void base64RemainsTheDefaultDecimalFormat() {
        Schema schema = SchemaBuilder.struct().field("amount", Decimal.schema(2)).build();
        Struct value = new Struct(schema).put("amount", new BigDecimal("12.34"));

        assertEquals("{\"amount\":\"BNI=\"}", json(decimalFormatter("BASE64"), schema, value));
    }

    @Test
    public void anUnknownDecimalFormatIsRejectedAtConfigurationTime() {
        ConfigException ex = assertThrows(ConfigException.class, () -> decimalFormatter("plain"));
        assertEquals(true, ex.getMessage().contains(OutputJsonFormatterConfig.DECIMAL_FORMAT_CONFIG));
    }

    private static OutputJsonFormatter decimalFormatter(String format) {
        OutputJsonFormatter formatter = new OutputJsonFormatter();
        Map<String, Object> config = new HashMap<>();
        config.put(OutputJsonFormatterConfig.DECIMAL_FORMAT_CONFIG, format);
        formatter.configure(config);
        return formatter;
    }

    @Test
    public void requiredFieldWithNullValueIsRejected() {
        Schema schema = SchemaBuilder.struct().field("required", Schema.STRING_SCHEMA).build();
        // Bypass Struct.validate by disabling null skipping, so the converter itself has to reject the value.
        OutputJsonFormatter formatter = new OutputJsonFormatter();
        Map<String, Object> config = new HashMap<>();
        config.put(OutputJsonFormatterConfig.NULL_CONFIG, "false");
        formatter.configure(config);

        assertThrows(DataException.class, () -> json(formatter, schema, new Struct(schema)));
    }

    @Test
    public void formatterInstancesDoNotShareFormattingState() {
        // The date formats used to be static, so a second connector in the worker inherited the first one's config.
        Schema schema = SchemaBuilder.struct().field("d", Date.SCHEMA).build();
        Struct value = new Struct(schema).put("d", Date.toLogical(Date.SCHEMA, 18417));

        OutputJsonFormatter custom = new OutputJsonFormatter();
        Map<String, Object> config = new HashMap<>();
        config.put(OutputJsonFormatterConfig.FORMATS_DATE_CONFIG, "dd/MM/yyyy");
        custom.configure(config);

        assertEquals("{\"d\":\"04/06/2020\"}", json(custom, schema, value));
        assertEquals("{\"d\":\"2020-06-04\"}", json(new OutputJsonFormatter(), schema, value));
    }
}
