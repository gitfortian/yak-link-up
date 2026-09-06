package com.link.up.connector.mongodb.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class MongoSinkConfigTest {

    @Test
    public void usesDatabaseFromUriAndBoundedDefaults() {
        MongoSinkConfig config = MongoSinkConfig.of(
                ReadonlyConfig.fromMap(base("mongodb://localhost:27017/app")));

        assertEquals("app", config.getDatabase());
        assertEquals("archive", config.getCollection());
        assertEquals(1000, config.getBatchSize());
        assertNull(config.getDocumentIdField());
    }

    @Test
    public void explicitDatabaseAndDocumentIdOverrideDefaults() {
        Map<String, Object> values = base("mongodb://localhost:27017/from_uri");
        values.put("database", "target_db");
        values.put("document_id_field", "order_id");
        values.put("batch_size", 256);

        MongoSinkConfig config = MongoSinkConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals("target_db", config.getDatabase());
        assertEquals("order_id", config.getDocumentIdField());
        assertEquals(256, config.getBatchSize());
    }

    @Test
    public void requiresDatabaseWhenUriHasNone() {
        assertInvalid(base("mongodb://localhost:27017"));
    }

    @Test
    public void rejectsNonPositiveBatchSize() {
        Map<String, Object> values = base("mongodb://localhost:27017/app");
        values.put("batch_size", 0);
        assertInvalid(values);
    }

    private static Map<String, Object> base(String uri) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("uri", uri);
        values.put("collection", "archive");
        return values;
    }

    private static void assertInvalid(Map<String, Object> values) {
        try {
            MongoSinkConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected invalid MongoDB Sink configuration");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }
}
