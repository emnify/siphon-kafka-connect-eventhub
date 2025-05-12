package com.microsoft.azure.eventhubs.kafka.connect.sink;

import org.apache.kafka.connect.data.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;

@RunWith(MockitoJUnitRunner.class)
public class OutputJsonSerializerTest {


    @Test
    public void testSimpleConversion() {
        Schema simpleSchema = SchemaBuilder.struct()
                .field("textField", Schema.STRING_SCHEMA).field("fieldNull", Schema.OPTIONAL_STRING_SCHEMA)
                .field("boolField", Schema.BOOLEAN_SCHEMA).field("timestampField", SchemaBuilder.int64().name(Timestamp.LOGICAL_NAME))
                .build();
        Struct simpleStruct = new Struct(simpleSchema);
        Instant now = Instant.ofEpochMilli(1591277777159L);
        java.util.Date nowDate = java.util.Date.from(now);

        simpleStruct.put("textField", "test");
        simpleStruct.put("boolField", false);
        simpleStruct.put("timestampField", nowDate);

        OutputJsonFormatter serializer = new OutputJsonFormatter();
        byte[] data = serializer.fromConnectData("test", simpleSchema, simpleStruct);
        String dataStr = new String(data);
        assert dataStr.contentEquals("{\"textField\":\"test\",\"boolField\":false,\"timestampField\":\"2020-06-04T13:36:17.159\"}");
    }

}
