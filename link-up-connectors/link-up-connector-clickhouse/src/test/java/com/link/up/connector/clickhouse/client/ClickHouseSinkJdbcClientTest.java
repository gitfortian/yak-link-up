package com.link.up.connector.clickhouse.client;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.clickhouse.config.ClickHouseSinkConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ClickHouseSinkJdbcClientTest {

    @Test
    public void buildsNamedPreparedInsertInPreparedMetadataOrder() {
        CatalogTable target =
                CatalogTable.builder(
                                TablePath.of("analytics", "orders"),
                                TableSchema.builder()
                                        .column(Column.builder("id", BasicType.LONG_TYPE).build())
                                        .column(Column.builder("order`name", BasicType.STRING_TYPE).build())
                                        .build())
                        .build();

        assertEquals(
                "INSERT INTO `analytics`.`orders` (`id`, `order``name`) VALUES (?, ?)",
                ClickHouseSinkJdbcClient.buildInsertSql(target));
    }

    @Test
    public void jdbcUrlPinsSynchronousInsertBoundary() {
        ClickHouseSinkConfig config = config();
        assertEquals(
                "jdbc:clickhouse:http://ch-1:8123/analytics?async_insert=0&wait_for_async_insert=1",
                ClickHouseSinkJdbcClient.buildSafeJdbcUrl(config));
    }

    private static ClickHouseSinkConfig config() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("host", "ch-1:8123");
        values.put("username", "default");
        values.put("password", "");
        values.put("database", "analytics");
        values.put("table", "orders");
        return ClickHouseSinkConfig.of(ReadonlyConfig.fromMap(values));
    }
}
