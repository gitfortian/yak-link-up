package com.link.up.connector.clickhouse.client;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.connector.clickhouse.config.ClickHouseSinkConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClickHouseSinkJdbcClientTest {

    @Test
    public void buildsInputFunctionPreparedInsertFromStableFluxTypes() {
        TableSchema source =
                TableSchema.builder()
                        .column(Column.builder("id", BasicType.LONG_TYPE).nullable(false).build())
                        .column(Column.builder("name", BasicType.STRING_TYPE).nullable(true).build())
                        .build();
        CatalogTable target =
                CatalogTable.builder(
                                TablePath.of("analytics", "orders"),
                                TableSchema.builder()
                                        .column(Column.builder("id", BasicType.STRING_TYPE).sourceType("Int128").build())
                                        .column(Column.builder("order`name", BasicType.STRING_TYPE).sourceType("JSON").build())
                                        .build())
                        .build();

        assertEquals(
                "INSERT INTO `analytics`.`orders` (`id`, `order``name`) SELECT CAST(c0 AS Int128), CAST(c1 AS JSON) FROM input('c0 Int64, c1 Nullable(String)')",
                ClickHouseSinkJdbcClient.buildInsertSql(target, source));
    }

    @Test
    public void mapsFluxInputTypesWithoutDependingOnTargetTypeParser() {
        assertEquals(
                "UInt8",
                ClickHouseSinkJdbcClient.inputType(
                        Column.builder("flag", BasicType.BOOLEAN_TYPE).nullable(false).build()));
        assertEquals(
                "Nullable(Decimal(20,0))",
                ClickHouseSinkJdbcClient.inputType(
                        Column.builder("u64", new DecimalType(20, 0)).nullable(true).build()));
        assertEquals(
                "DateTime64(9)",
                ClickHouseSinkJdbcClient.inputType(
                        Column.builder("ts", BasicType.TIMESTAMP_TYPE).nullable(false).build()));
    }

    @Test
    public void jdbcUrlPinsSynchronousInsertBoundary() {
        ClickHouseSinkConfig config = config();
        assertEquals(
                "jdbc:clickhouse:http://ch-1:8123/analytics?async_insert=0&wait_for_async_insert=1",
                ClickHouseSinkJdbcClient.buildSafeJdbcUrl(config));
    }

    @Test
    public void materializedAliasAndEphemeralColumnsAreNotWritable() {
        assertTrue(ClickHouseSinkJdbcClient.isWritableColumn(""));
        assertTrue(ClickHouseSinkJdbcClient.isWritableColumn("DEFAULT"));
        assertFalse(ClickHouseSinkJdbcClient.isWritableColumn("MATERIALIZED"));
        assertFalse(ClickHouseSinkJdbcClient.isWritableColumn("alias"));
        assertFalse(ClickHouseSinkJdbcClient.isWritableColumn("EPHEMERAL"));
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
