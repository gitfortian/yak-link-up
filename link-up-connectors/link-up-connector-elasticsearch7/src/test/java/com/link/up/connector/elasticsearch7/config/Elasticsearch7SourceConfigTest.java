package com.link.up.connector.elasticsearch7.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class Elasticsearch7SourceConfigTest {

    @Test
    public void defaultsStayBoundedAndUseFrameworkParallelism() {
        Elasticsearch7SourceConfig config = config("orders");
        assertEquals(Arrays.asList("http://localhost:9200"), config.getHosts());
        assertEquals(60000L, config.getScrollTimeMillis());
        assertEquals(1000, config.getScrollSize());
        assertNull(config.getSlices());
        assertEquals(4, config.resolveSlices(4));
    }

    @Test
    public void explicitSlicesOverrideExecutionParallelism() {
        Map<String, Object> values = base("orders");
        values.put("slices", 3);
        Elasticsearch7SourceConfig config = Elasticsearch7SourceConfig.of(ReadonlyConfig.fromMap(values));
        assertEquals(3, config.resolveSlices(8));
    }

    @Test
    public void rejectsWildcardAndMultiIndexExpressions() {
        assertInvalidIndex("orders-*");
        assertInvalidIndex("orders,customers");
        assertInvalidIndex("orders-?");
    }

    @Test
    public void parsesMillisecondDuration() {
        Map<String, Object> values = base("orders");
        values.put("scroll_time", "1500ms");
        Elasticsearch7SourceConfig config = Elasticsearch7SourceConfig.of(ReadonlyConfig.fromMap(values));
        assertEquals(1500L, config.getScrollTimeMillis());
    }

    private static void assertInvalidIndex(String index) {
        try {
            Elasticsearch7SourceConfig.of(ReadonlyConfig.fromMap(base(index)));
            fail("Expected invalid Stage 1 index expression: " + index);
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    private static Elasticsearch7SourceConfig config(String index) {
        return Elasticsearch7SourceConfig.of(ReadonlyConfig.fromMap(base(index)));
    }

    private static Map<String, Object> base(String index) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("hosts", Arrays.asList("http://localhost:9200"));
        values.put("index", index);
        return values;
    }
}
