package com.link.up.connector.mongodb.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoSourceConfigTest {

    @Test
    public void usesDatabaseFromUriByDefault() {
        MongoSourceConfig config = MongoSourceConfig.of(
                ReadonlyConfig.fromMap(base("mongodb://localhost:27017/app")));

        assertEquals("app", config.getDatabase());
        assertEquals("users", config.getCollection());
        assertEquals(1000, config.getFetchSize());
        assertTrue(config.getFields().isEmpty());
    }

    @Test
    public void explicitDatabaseOverridesUriDatabase() {
        Map<String, Object> values = base("mongodb://localhost:27017/from_uri");
        values.put("database", "selected_db");
        values.put("fields", Arrays.asList("_id", "address.city"));
        values.put("filter", "{\"active\": true}");
        values.put("fetch_size", 256);

        MongoSourceConfig config = MongoSourceConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals("selected_db", config.getDatabase());
        assertEquals(Arrays.asList("_id", "address.city"), config.getFields());
        assertEquals(256, config.getFetchSize());
        assertEquals(true, config.createFilter().getBoolean("active").getValue());
    }

    @Test
    public void requiresDatabaseWhenUriHasNone() {
        assertInvalid(base("mongodb://localhost:27017"));
    }

    @Test
    public void rejectsDuplicateFields() {
        Map<String, Object> values = base("mongodb://localhost:27017/app");
        values.put("fields", Arrays.asList("name", "name"));
        assertInvalid(values);
    }

    @Test
    public void rejectsInvalidFilter() {
        Map<String, Object> values = base("mongodb://localhost:27017/app");
        values.put("filter", "{not-json}");
        assertInvalid(values);
    }

    private static Map<String, Object> base(String uri) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("uri", uri);
        values.put("collection", "users");
        return values;
    }

    private static void assertInvalid(Map<String, Object> values) {
        try {
            MongoSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected invalid MongoDB Source configuration");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }
}
