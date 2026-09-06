package com.link.up.connector.elasticsearch8.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class Elasticsearch8SinkConfigTest {

    @Test
    public void usesBoundedSinkDefaults() {
        Elasticsearch8SinkConfig config = config("orders_target", null);
        assertEquals(1000, config.getBatchSize());
        assertEquals(3, config.getMaxRetries());
        assertEquals(200L, config.getRetryBackoffMs());
        assertEquals(5000L, config.getMaxRetryBackoffMs());
        assertEquals(10000, config.getConnectTimeoutMs());
        assertEquals(60000, config.getSocketTimeoutMs());
        assertNull(config.getDocumentIdField());
    }

    @Test
    public void keepsStableDocumentIdAndCapsBackoff() {
        Elasticsearch8SinkConfig config = config("orders_target", "order_id");
        assertEquals("order_id", config.getDocumentIdField());
        assertEquals(200L, config.retryBackoffMs(0));
        assertEquals(400L, config.retryBackoffMs(1));
        assertEquals(5000L, config.retryBackoffMs(20));
    }

    @Test
    public void rejectsWildcardTarget() {
        try {
            config("orders-*", null);
            fail("Expected wildcard target rejection");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    private static Elasticsearch8SinkConfig config(String index, String idField) {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("hosts", Arrays.asList("http://localhost:9200"));
        values.put("index", index);
        if (idField != null) {
            values.put("document_id_field", idField);
        }
        return Elasticsearch8SinkConfig.of(ReadonlyConfig.fromMap(values));
    }
}
