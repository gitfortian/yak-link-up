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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClickHouseSinkJdbcClientTest {

    @Test
    public void buildsInputFunctionPreparedInsertInPreparedMetadataOrder() {
        CatalogTable target =
                CatalogTable.builder(
                                TablePath.of("analytics", "orders"),
                                TableSchema.builder()
                                        .column(Column.builder("id", BasicType.LONG_TYPE).sourceType("Int64").build())
                                        .column(Column.builder("order`name", BasicType.STRING_TYPE).sourceType("Nullable(String)").build())
                                        .build())
                        .build();

        assertEquals(
                "INSERT INTO `analytics`.`orders` (`id`, `order``name`) SELECT c0, c1 FROM input('c0 Int64, c1 Nullable(String)')",
                ClickHouseSinkJdbcClient.buildInsertSql(target));
    }

    @Test
    public void escapesQuotesInsideInputTypeDeclaration() {
        CatalogTable target =
                CatalogTable.builder(
                                TablePath.of("analytics", "orders"),
                                TableSchema.builder()
                                        .column(
                                                Column.builder("kind", BasicType.STRING_TYPE)
                                                        .sourceType("Enum8('a' = 1, 'b' = 2)")
                                                        .build())
                                        .build())
                        .build();

        assertEquals(
                "INSERT INTO `analytics`.`orders` (`kind`) SELECT c0 FROM input('c0 Enum8(\\'a\\' = 1, \\'b\\' = 2)')",
                ClickHouseSinkJdbcClient.buildInsertSql(target));
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
