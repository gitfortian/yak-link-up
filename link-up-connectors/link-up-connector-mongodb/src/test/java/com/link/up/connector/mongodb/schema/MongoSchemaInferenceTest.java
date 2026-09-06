package com.link.up.connector.mongodb.schema;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.SqlType;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;

public class MongoSchemaInferenceTest {

    @Test
    public void infersPortableTypesAndNestedPaths() {
        MongoSchemaInference inference = new MongoSchemaInference(8);

        BsonDocument first = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId("64b64c8f0000000000000001")))
                .append("name", new BsonString("Yak"))
                .append("age", new BsonInt32(18))
                .append("score", new BsonDouble(9.5D))
                .append("created_at", new BsonDateTime(1700000000000L))
                .append("address", new BsonDocument("city", new BsonString("Chengdu")))
                .append("tags", new BsonArray(Arrays.<BsonValue>asList(new BsonString("data"))));

        BsonDocument second = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId("64b64c8f0000000000000002")))
                .append("name", new BsonString("Link-Up"))
                .append("age", new BsonInt64(20L))
                .append("score", new BsonDouble(9.8D))
                .append("created_at", new BsonDateTime(1700000100000L))
                .append("address", new BsonDocument("province", new BsonString("Sichuan")))
                .append("tags", new BsonArray(Arrays.<BsonValue>asList(new BsonString("sync"))));

        inference.observe(first);
        inference.observe(second);
        TableSchema schema = inference.build();

        Assert.assertEquals(SqlType.STRING, schema.getColumn("_id").getDataType().getSqlType());
        Assert.assertEquals(SqlType.STRING, schema.getColumn("name").getDataType().getSqlType());
        Assert.assertEquals(SqlType.BIGINT, schema.getColumn("age").getDataType().getSqlType());
        Assert.assertEquals(SqlType.DOUBLE, schema.getColumn("score").getDataType().getSqlType());
        Assert.assertEquals(SqlType.TIMESTAMP, schema.getColumn("created_at").getDataType().getSqlType());
        Assert.assertEquals(SqlType.STRING, schema.getColumn("address").getDataType().getSqlType());
        Assert.assertEquals(SqlType.STRING, schema.getColumn("address.city").getDataType().getSqlType());
        Assert.assertEquals(SqlType.STRING, schema.getColumn("address.province").getDataType().getSqlType());
        Assert.assertEquals(SqlType.STRING, schema.getColumn("tags").getDataType().getSqlType());

        Assert.assertFalse(schema.getColumn("_id").isNullable());
        Assert.assertTrue(schema.getColumn("address.city").isNullable());
        Assert.assertTrue(schema.getColumn("address.province").isNullable());
        Assert.assertEquals("ObjectId", schema.getColumn("_id").getSourceType());
        Assert.assertEquals("true", schema.getColumn("address").getAttributes().get("mongodb.complex"));
        Assert.assertEquals("true", schema.getColumn("address.city").getAttributes().get("mongodb.nested"));
        Assert.assertEquals(Arrays.asList("_id"), schema.getPrimaryKey().getColumnNames());
    }

    @Test
    public void widensNumbersAndFallsBackOnIncompatibleValues() {
        MongoSchemaInference inference = new MongoSchemaInference(8);

        inference.observe(new BsonDocument()
                .append("count", new BsonInt32(1))
                .append("price", new BsonDecimal128(new Decimal128(new BigDecimal("12.34"))))
                .append("mixed", new BsonInt32(1)));
        inference.observe(new BsonDocument()
                .append("count", new BsonDouble(2.5D))
                .append("price", new BsonInt64(15L))
                .append("mixed", new BsonString("unknown")));

        TableSchema schema = inference.build();
        Assert.assertEquals(SqlType.DOUBLE, schema.getColumn("count").getDataType().getSqlType());
        Assert.assertEquals(SqlType.DECIMAL, schema.getColumn("price").getDataType().getSqlType());
        Assert.assertEquals(SqlType.STRING, schema.getColumn("mixed").getDataType().getSqlType());
        Assert.assertEquals("true", schema.getColumn("mixed").getAttributes().get("mongodb.heterogeneous"));
    }

    @Test
    public void marksMissingFieldsNullable() {
        MongoSchemaInference inference = new MongoSchemaInference(8);
        inference.observe(new BsonDocument("name", new BsonString("Yak")));
        inference.observe(new BsonDocument("other", new BsonString("value")));

        TableSchema schema = inference.build();
        Assert.assertTrue(schema.getColumn("name").isNullable());
        Assert.assertTrue(schema.getColumn("other").isNullable());
    }

    @Test
    public void keepsMetadataUsableForEmptyCollection() {
        TableSchema schema = new MongoSchemaInference(8).build();
        Column id = schema.getColumn("_id");

        Assert.assertEquals(SqlType.STRING, id.getDataType().getSqlType());
        Assert.assertFalse(id.isNullable());
        Assert.assertEquals("true", id.getAttributes().get("mongodb.synthetic"));
        Assert.assertEquals(Arrays.asList("_id"), schema.getPrimaryKey().getColumnNames());
    }
}
