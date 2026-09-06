package com.link.up.connector.doris.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DorisSourceConfigTest {

    @Test
    public void parsesSingleTableWithSeaTunnelCompatibleDefaults() {
        Map<String, Object> values = base();
        values.put("database", "analytics");
        values.put("table", "orders");
        values.put("doris.read.field", "id, amount");
        values.put("doris.filter.query", "id >= 10");

        DorisSourceConfig config = DorisSourceConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals(Arrays.asList("fe-1:8030", "fe-2:8030"), config.getFeNodes());
        assertEquals(9030, config.getQueryPort());
        assertEquals(30_000, config.getConnectTimeoutMs());
        assertEquals(30_000, config.getReadTimeoutMs());
        assertEquals(3600, config.getQueryTimeoutSec());
        assertEquals(3, config.getRequestRetries());

        DorisSourceTableConfig table = config.getTableConfigs().get(0);
        assertEquals("analytics", table.getDatabase());
        assertEquals("orders", table.getTable());
        assertEquals(Arrays.asList("id", "amount"), table.getReadFields());
        assertEquals("id >= 10", table.getFilterQuery());
        assertEquals(Integer.MAX_VALUE, table.getRequestTabletSize());
        assertEquals(1024, table.getBatchSize());
        assertEquals(2_147_483_648L, table.getExecMemLimit());
    }

    @Test
    public void tableListSupportsPerTableDatabaseAndScanOverrides() {
        Map<String, Object> values = base();

        Map<String, Object> first = new LinkedHashMap<String, Object>();
        first.put("database", "db_a");
        first.put("table", "orders");
        first.put("doris.read.field", "id,total");
        first.put("doris.request.tablet.size", 2);
        first.put("doris.batch.size", 256);
        first.put("doris.exec.mem.limit", 1024L * 1024L);

        Map<String, Object> second = new LinkedHashMap<String, Object>();
        second.put("database", "db_b");
        second.put("table", "customers");
        second.put("doris.filter.query", "active = 1");

        values.put("table_list", Arrays.asList(first, second));
        DorisSourceConfig config = DorisSourceConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals(2, config.getTableConfigs().size());
        assertEquals("db_a", config.getTableConfigs().get(0).getDatabase());
        assertEquals(2, config.getTableConfigs().get(0).getRequestTabletSize());
        assertEquals(256, config.getTableConfigs().get(0).getBatchSize());
        assertEquals("db_b", config.getTableConfigs().get(1).getDatabase());
        assertEquals("active = 1", config.getTableConfigs().get(1).getFilterQuery());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsMissingDatabaseForSingleTable() {
        Map<String, Object> values = base();
        values.put("table", "orders");
        DorisSourceConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTableAndTableListTogether() {
        Map<String, Object> values = base();
        values.put("database", "db");
        values.put("table", "orders");
        Map<String, Object> item = new LinkedHashMap<String, Object>();
        item.put("database", "db");
        item.put("table", "customers");
        values.put("table_list", Arrays.asList(item));
        DorisSourceConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test
    public void acceptsCompatibilityFallbackKeys() {
        Map<String, Object> values = base();
        values.put("database", "db");
        values.put("table", "t");
        values.put("request_tablet_size", 3);
        values.put("scan_batch_rows", 128);
        DorisSourceConfig config = DorisSourceConfig.of(ReadonlyConfig.fromMap(values));
        assertEquals(3, config.getTableConfigs().get(0).getRequestTabletSize());
        assertEquals(128, config.getTableConfigs().get(0).getBatchSize());
        assertTrue(config.getTableConfigs().get(0).getReadFields().isEmpty());
    }

    private static Map<String, Object> base() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("fenodes", "fe-1:8030,fe-2:8030");
        values.put("username", "root");
        values.put("password", "");
        return values;
    }
}
