package com.link.up.connector.elasticsearch8.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class Elasticsearch8SourceConfigTest {

    @Test
    public void usesBoundedDefaultsAndRuntimeParallelism() {
        Elasticsearch8SourceConfig config = Elasticsearch8SourceConfig.of(
                ReadonlyConfig.fromMap(base("orders")));
        assertEquals("orders", config.getIndex());
        assertEquals("1m", config.getScrollTime());
        assertEquals(1000, config.getScrollSize());
        assertNull(config.getSlices());
        assertEquals(3, config.resolveSlices(3));
    }

    @Test
    public void fixedSlicesOverrideRuntimeParallelism() {
        Map<String, Object> values = base("orders");
        values.put("slices", 4);
        Elasticsearch8SourceConfig config = Elasticsearch8SourceConfig.of(
                ReadonlyConfig.fromMap(values));
        assertEquals(4, config.resolveSlices(2));
    }

    @Test
    public void rejectsMultiIndexExpressions() {
        try {
            Elasticsearch8SourceConfig.of(ReadonlyConfig.fromMap(base("orders-*")));
            fail("Expected wildcard index rejection");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    @Test
    public void validatesScrollDuration() {
        Map<String, Object> values = base("orders");
        values.put("scroll_time", "30s");
        Elasticsearch8SourceConfig config = Elasticsearch8SourceConfig.of(
                ReadonlyConfig.fromMap(values));
        assertEquals("30s", config.getScrollTime());

        values.put("scroll_time", "forever");
        try {
            Elasticsearch8SourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected invalid duration rejection");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    private static Map<String, Object> base(String index) {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("hosts", Arrays.asList("http://localhost:9200"));
        values.put("index", index);
        return values;
    }
}
