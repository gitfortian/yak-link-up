package com.link.up.connector.clickhouse.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClickHouseSourceConfigTest {

    @Test
    public void parsesSingleTableWithDefaultOptions() {
        Map<String, Object> values = base();
        values.put("table_path", "analytics.orders");

        ClickHouseSourceConfig config = ClickHouseSourceConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals(Arrays.asList("fe-1:8123", "https://fe-2:8443"), config.getHosts());
        assertEquals(1, config.getTableConfigs().size());
        ClickHouseSourceTableConfig table = config.getTableConfigs().get(0);
        assertEquals("analytics.orders", table.getTablePath().toString());
        assertEquals(Integer.MAX_VALUE, table.getSplitSize());
        assertEquals(1024, table.getBatchSize());
        assertFalse(table.isSqlMode());
    }

    @Test
    public void parsesMultiTableOverrides() {
        Map<String, Object> values = base();
        values.put("filter_query", "tenant_id > 0");

        List<Map<String, Object>> tableList = new ArrayList<Map<String, Object>>();
        Map<String, Object> orders = new LinkedHashMap<String, Object>();
        orders.put("table_path", "analytics.orders");
        orders.put("split_size", 2);
        orders.put("batch_size", 256);
        orders.put("partition_list", Arrays.asList("20260905", "20260906"));
        tableList.add(orders);

        Map<String, Object> customers = new LinkedHashMap<String, Object>();
        customers.put("table_path", "crm.customers");
        customers.put("filter_query", "active = 1");
        tableList.add(customers);
        values.put("table_list", tableList);

        ClickHouseSourceConfig config = ClickHouseSourceConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals(2, config.getTableConfigs().size());
        ClickHouseSourceTableConfig first = config.getTableConfigs().get(0);
        assertEquals(2, first.getSplitSize());
        assertEquals(256, first.getBatchSize());
        assertEquals(Arrays.asList("20260905", "20260906"), first.getPartitionList());
        assertEquals("tenant_id > 0", first.getFilterQuery());

        ClickHouseSourceTableConfig second = config.getTableConfigs().get(1);
        assertEquals("active = 1", second.getFilterQuery());
        assertEquals(Integer.MAX_VALUE, second.getSplitSize());
    }

    @Test
    public void supportsSqlOnlyDatasetWithSyntheticPath() {
        Map<String, Object> values = base();
        values.put("sql", "select id, amount from analytics.orders");

        ClickHouseSourceConfig config = ClickHouseSourceConfig.of(ReadonlyConfig.fromMap(values));
        ClickHouseSourceTableConfig table = config.getTableConfigs().get(0);

        assertTrue(table.isSqlMode());
        assertTrue(table.isSyntheticTablePath());
        assertEquals("__query__.clickhouse_query_0", table.getTablePath().toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsTableListCombinedWithTopLevelTable() {
        Map<String, Object> values = base();
        values.put("table_path", "analytics.orders");
        Map<String, Object> item = new LinkedHashMap<String, Object>();
        item.put("table_path", "crm.customers");
        values.put("table_list", Arrays.asList(item));
        ClickHouseSourceConfig.of(ReadonlyConfig.fromMap(values));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsPartitionListForSqlMode() {
        Map<String, Object> values = base();
        values.put("sql", "select * from analytics.orders");
        values.put("partition_list", Arrays.asList("20260906"));
        ClickHouseSourceConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static Map<String, Object> base() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("host", "fe-1:8123, https://fe-2:8443/");
        values.put("username", "default");
        values.put("password", "");
        return values;
    }
}
