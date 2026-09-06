package com.link.up.connector.clickhouse.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ClickHouseSinkConfigTest {

    @Test
    public void parsesBoundedSinkDefaults() {
        ClickHouseSinkConfig config = ClickHouseSinkConfig.of(ReadonlyConfig.fromMap(base()));

        assertEquals("ch-1:8123", config.getHost());
        assertEquals("analytics", config.getDatabase());
        assertEquals("orders", config.getTable());
        assertEquals(10000, config.getBatchSize());
        assertEquals("analytics.orders", config.getTargetPath().toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsMultipleSinkHosts() {
        Map<String, Object> values = base();
        values.put("host", "ch-1:8123,ch-2:8123");
        ClickHouseSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsFireAndForgetAsyncAcknowledgement() {
        Map<String, Object> values = base();
        Map<String, String> clientConfig = new LinkedHashMap<String, String>();
        clientConfig.put("wait_for_async_insert", "0");
        values.put("clickhouse.config", clientConfig);
        ClickHouseSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsAsyncInsertBecauseWriterOwnsBatchBoundary() {
        Map<String, Object> values = base();
        Map<String, String> clientConfig = new LinkedHashMap<String, String>();
        clientConfig.put("async_insert", "1");
        values.put("clickhouse.config", clientConfig);
        ClickHouseSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static Map<String, Object> base() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("host", "ch-1:8123/");
        values.put("username", "default");
        values.put("password", "");
        values.put("database", "analytics");
        values.put("table", "orders");
        return values;
    }
}
