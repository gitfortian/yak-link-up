package com.link.up.connector.mongodb.converter;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoBsonRowConverterTest {

    @Test
    public void convertsPortableScalarAndNestedValues() {
        TableSchema schema = TableSchema.builder()
                .column(column("_id", BasicType.STRING_TYPE, "_id"))
                .column(column("age", BasicType.LONG_TYPE, "age"))
                .column(column("score", BasicType.DOUBLE_TYPE, "score"))
                .column(column("created_at", BasicType.TIMESTAMP_TYPE, "created_at"))
                .column(column("address.city", BasicType.STRING_TYPE, "address.city"))
                .column(column("payload", BasicType.STRING_TYPE, "payload"))
                .column(column("binary", BasicType.BYTES_TYPE, "binary"))
                .build();

        ObjectId id = new ObjectId("64b7abdecf2160b649ab6085");
        BsonDocument document = new BsonDocument()
                .append("_id", new BsonObjectId(id))
                .append("age", new BsonInt32(18))
                .append("score", new BsonDouble(9.5D))
                .append("created_at", new BsonDateTime(0L))
                .append(
                        "address",
                        new BsonDocument("city", new BsonString("Chengdu")))
                .append(
                        "payload",
                        new BsonArray(Arrays.<BsonValue>asList(
                                new BsonInt32(1),
                                new BsonString("x"))))
                .append("binary", new BsonBinary(new byte[]{1, 2, 3}));

        FluxRow row = new MongoBsonRowConverter(schema).convert(document);

        assertEquals(id.toHexString(), row.getField(0));
        assertEquals(Long.valueOf(18L), row.getField(1));
        assertEquals(Double.valueOf(9.5D), row.getField(2));
        assertEquals(LocalDateTime.of(1970, 1, 1, 0, 0), row.getField(3));
        assertEquals("Chengdu", row.getField(4));
        assertTrue(((String) row.getField(5)).startsWith("["));
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) row.getField(6));
    }

    @Test
    public void missingNestedFieldBecomesNull() {
        TableSchema schema = TableSchema.builder()
                .column(column("address.city", BasicType.STRING_TYPE, "address.city"))
                .build();

        FluxRow row = new MongoBsonRowConverter(schema)
                .convert(new BsonDocument("address", new BsonDocument()));

        assertEquals(null, row.getField(0));
    }

    @Test
    public void stringFallbackCarriesHeterogeneousScalarAsText() {
        TableSchema schema = TableSchema.builder()
                .column(column("value", BasicType.STRING_TYPE, "value"))
                .build();

        FluxRow row = new MongoBsonRowConverter(schema)
                .convert(new BsonDocument("value", new BsonInt32(42)));

        assertEquals("42", row.getField(0));
    }

    @Test
    public void typedSchemaDriftFailsInsteadOfSilentlyCoercing() {
        TableSchema schema = TableSchema.builder()
                .column(column("age", BasicType.INT_TYPE, "age"))
                .build();

        try {
            new MongoBsonRowConverter(schema)
                    .convert(new BsonDocument("age", new BsonString("unknown")));
            fail("Expected runtime schema drift to fail");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("field=age"));
            assertTrue(expected.getMessage().contains("actual=STRING"));
        }
    }

    private static Column column(
            String name,
            com.link.up.api.table.type.FluxDataType<?> type,
            String path) {
        return Column.builder(name, type)
                .attribute("mongodb.path", path)
                .build();
    }
}
