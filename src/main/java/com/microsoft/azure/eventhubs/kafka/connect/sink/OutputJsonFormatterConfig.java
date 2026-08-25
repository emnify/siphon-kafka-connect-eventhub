package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.storage.ConverterConfig;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Formatting options for the JSON representation of records that carry a Connect schema.
 *
 * <p>Every option here is applied. An earlier revision defined the date and datetime patterns but never wired them
 * up, so a connector configured with {@code json.datetime.pattern} silently emitted the default format instead.
 */
public class OutputJsonFormatterConfig extends AbstractConfig {

    public static final String FORMATS_DATE_TIME_CONFIG = "json.datetime.pattern";
    public static final String FORMATS_DATE_TIME_DEFAULT = "";
    private static final String FORMATS_DATE_TIME_DOC =
            "DateTimeFormatter pattern for Timestamp logical types. Empty means ISO_LOCAL_DATE_TIME";
    private static final String FORMATS_DATE_TIME_DISPLAY = "Datetime format";

    public static final String FORMATS_DATE_CONFIG = "json.date.pattern";
    public static final String FORMATS_DATE_DEFAULT = "";
    private static final String FORMATS_DATE_DOC =
            "DateTimeFormatter pattern for Date logical types. Empty means ISO_LOCAL_DATE";
    private static final String FORMATS_DATE_DISPLAY = "Date format";

    public static final String FORMATS_TIME_CONFIG = "json.time.pattern";
    public static final String FORMATS_TIME_DEFAULT = "";
    private static final String FORMATS_TIME_DOC =
            "DateTimeFormatter pattern for Time logical types. Empty means ISO_LOCAL_TIME";
    private static final String FORMATS_TIME_DISPLAY = "Time format";

    public static final String TIME_ZONE_CONFIG = "json.timestamp.zone";
    public static final String TIME_ZONE_DEFAULT = "UTC";
    private static final String TIME_ZONE_DOC =
            "Zone used to render Timestamp logical types. Changing this changes the emitted values";
    private static final String TIME_ZONE_DISPLAY = "Timestamp zone";

    public static final String NULL_CONFIG = "json.skip.null";
    public static final boolean NULL_DEFAULT = true;
    private static final String NULL_DOC = "Skip null fields while converting to JSON";
    private static final String NULL_DISPLAY = "Skip JSON nulls";

    private static final ConfigDef CONFIG;

    static {
        String group = "Schemas";
        int orderInGroup = 0;
        CONFIG = ConverterConfig.newConfigDef();
        CONFIG.define(FORMATS_DATE_TIME_CONFIG, ConfigDef.Type.STRING, FORMATS_DATE_TIME_DEFAULT,
                ConfigDef.Importance.LOW, FORMATS_DATE_TIME_DOC, group, orderInGroup++, ConfigDef.Width.MEDIUM,
                FORMATS_DATE_TIME_DISPLAY);
        CONFIG.define(FORMATS_DATE_CONFIG, ConfigDef.Type.STRING, FORMATS_DATE_DEFAULT,
                ConfigDef.Importance.LOW, FORMATS_DATE_DOC, group, orderInGroup++, ConfigDef.Width.MEDIUM,
                FORMATS_DATE_DISPLAY);
        CONFIG.define(FORMATS_TIME_CONFIG, ConfigDef.Type.STRING, FORMATS_TIME_DEFAULT,
                ConfigDef.Importance.LOW, FORMATS_TIME_DOC, group, orderInGroup++, ConfigDef.Width.MEDIUM,
                FORMATS_TIME_DISPLAY);
        CONFIG.define(TIME_ZONE_CONFIG, ConfigDef.Type.STRING, TIME_ZONE_DEFAULT,
                ConfigDef.Importance.LOW, TIME_ZONE_DOC, group, orderInGroup++, ConfigDef.Width.MEDIUM,
                TIME_ZONE_DISPLAY);
        CONFIG.define(NULL_CONFIG, ConfigDef.Type.BOOLEAN, NULL_DEFAULT,
                ConfigDef.Importance.LOW, NULL_DOC, group, orderInGroup++, ConfigDef.Width.MEDIUM,
                NULL_DISPLAY);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public OutputJsonFormatterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

    /** Format for Timestamp logical types. */
    public DateTimeFormatter dateTimeFormat() {
        return formatter(FORMATS_DATE_TIME_CONFIG, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    /** Format for Date logical types. */
    public DateTimeFormatter dateFormat() {
        return formatter(FORMATS_DATE_CONFIG, DateTimeFormatter.ISO_LOCAL_DATE);
    }

    /** Format for Time logical types. */
    public DateTimeFormatter timeFormat() {
        return formatter(FORMATS_TIME_CONFIG, DateTimeFormatter.ISO_LOCAL_TIME);
    }

    /** Zone used to render Timestamp logical types. */
    public ZoneId timestampZone() {
        String zone = getString(TIME_ZONE_CONFIG);
        try {
            return ZoneId.of(zone);
        } catch (RuntimeException ex) {
            throw new ConfigException(TIME_ZONE_CONFIG, zone, "is not a valid zone id");
        }
    }

    /** When enabled, null values are omitted from the serializer output rather than emitted as JSON null. */
    public boolean enableSkipNulls() {
        return getBoolean(NULL_CONFIG);
    }

    private DateTimeFormatter formatter(String key, DateTimeFormatter fallback) {
        String pattern = getString(key);
        if (pattern == null || pattern.isEmpty()) {
            return fallback;
        }
        try {
            return DateTimeFormatter.ofPattern(pattern);
        } catch (IllegalArgumentException ex) {
            // Fail at startup with the offending key rather than on the first record that uses it.
            throw new ConfigException(key, pattern, "is not a valid DateTimeFormatter pattern: " + ex.getMessage());
        }
    }
}
