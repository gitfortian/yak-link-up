package com.link.up.connector.mongodb.converter;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class MongoFluxRowBsonConverterTest {

    @Test
    public void restoresMongoObjectIdAndNestedDocument() {
        Column id = Column.builder("_id", BasicType.STRING_TYPE)
                .sourceType("ObjectId")
                .attribute("mongodb.bsonTypes", "OBJECT_ID")
                .build();
        Column address = Column.builder("address", BasicType.STRING_TYPE)
                .sourceType("Document")
                .attribute("mongodb.bsonTypes", "DOCUMENT")
                .attribute("mongodb.encoding", "extended-json")
                .attribute("mongodb.complex", "true")
                .build();
        Column city = Column.builder("address.city", BasicType.STRING_TYPE)
                .attribute("mongodb.path", "address.city")
                .build();
        Column createdAt = Column.builder("created_at", BasicType.TIMESTAMP_TYPE)
                .build();
        TableSchema schema = TableSchema.builder()
                .column(id)
                .column(address)
                .column(city)
                .column(createdAt)
                .build();

        String idValue = "64b7abdecf2160b649ab6085";
        FluxRow row = FluxRow.of(
                idValue,
                "{\"city\":\"Old\",\"province\":\"Sichuan\"}",
                "Chengdu",
                LocalDateTime.of(2026, 9, 6, 12, 30, 0, 123_000_000));

        BsonDocument document = new MongoFluxRowBsonConverter(schema, null).convert(row);

        assertEquals(new ObjectId(idValue), document.getObjectId("_id").getValue());
        assertEquals("Chengdu", document.getDocument("address").getString("city").getValue());
        assertEquals("Sichuan", document.getDocument("address").getString("province").getValue());
        assertEquals(BsonType.DATE_TIME, document.get("created_at").getBsonType());
    }

    @Test
    public void documentIdFieldCreatesScalarIdAndKeepsSourceField() {
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("order_id", BasicType.STRING_TYPE).build())
                .column(Column.builder("amount", BasicType.LONG_TYPE).build())
                .build();

        BsonDocument document = new MongoFluxRowBsonConverter(schema, "order_id")
                .convert(FluxRow.of("order-1", Long.valueOf(42L)));

        assertEquals("order-1", document.getString("_id").getValue());
        assertEquals("order-1", document.getString("order_id").getValue());
        assertEquals(42L, document.getInt64("amount").getValue());
    }

    @Test
    public void nullImplicitIdIsOmittedSoMongoCanGenerateOne() {
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("_id", BasicType.STRING_TYPE).nullable(true).build())
                .column(Column.builder("name", BasicType.STRING_TYPE).build())
                .build();

        BsonDocument document = new MongoFluxRowBsonConverter(schema, null)
                .convert(FluxRow.of(null, "Yak"));

        assertFalse(document.containsKey("_id"));
        assertEquals("Yak", document.getString("name").getValue());
    }

    @Test
    public void rejectsLossySubMillisecondTimestamp() {
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("created_at", BasicType.TIMESTAMP_TYPE).build())
                .build();

        try {
            new MongoFluxRowBsonConverter(schema, null)
                    .convert(FluxRow.of(LocalDateTime.of(2026, 9, 6, 12, 30, 0, 123_456_000)));
            fail("Expected sub-millisecond timestamp rejection");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    @Test
    public void explicitDocumentIdMustNotBeNull() {
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("order_id", BasicType.STRING_TYPE).nullable(true).build())
                .build();

        try {
            new MongoFluxRowBsonConverter(schema, "order_id")
                    .convert(FluxRow.of((Object) null));
            fail("Expected null document id rejection");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }
}
