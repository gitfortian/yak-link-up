package com.link.up.connector.clickhouse.source;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.clickhouse.config.ClickHouseSourceTableConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClickHouseSourceSqlBuilderTest {

    @Test
    public void buildsPartQueryWithProjectionAndFilter() {
        TablePath path = TablePath.of("analytics", "orders");
        TableSchema schema =
                TableSchema.builder()
                        .column(Column.builder("id", BasicType.LONG_TYPE).build())
                        .column(Column.builder("order_no", BasicType.STRING_TYPE).build())
                        .build();
        CatalogTable table = CatalogTable.builder(path, schema).build();
        ClickHouseSourceTableConfig config =
                new ClickHouseSourceTableConfig(
                        path,
                        false,
                        null,
                        "id >= 100",
                        Collections.<String>emptyList(),
                        10,
                        1024);
        ClickHouseSourceSplit split =
                new ClickHouseSourceSplit(
                        "split-1",
                        path,
                        "node-1:8123",
                        ClickHouseSourceSplit.Mode.PARTS,
                        Arrays.asList("202609_1_1_0", "202609_2_2_0"),
                        null);

        String sql = ClickHouseSourceSqlBuilder.build(split, config, table);

        assertEquals(
                "SELECT `id`, `order_no` FROM `analytics`.`orders` "
                        + "WHERE _part IN ('202609_1_1_0', '202609_2_2_0') AND (id >= 100)",
                sql);
    }

    @Test
    public void wrapsSqlAndAppliesAdditionalFilter() {
        TablePath path = TablePath.of("analytics", "orders");
        TableSchema schema =
                TableSchema.builder()
                        .column(Column.builder("id", BasicType.LONG_TYPE).build())
                        .build();
        CatalogTable table = CatalogTable.builder(path, schema).build();
        ClickHouseSourceTableConfig config =
                new ClickHouseSourceTableConfig(
                        path,
                        false,
                        "select id from analytics.orders;",
                        "id < 1000",
                        Collections.<String>emptyList(),
                        10,
                        1024);
        ClickHouseSourceSplit split =
                new ClickHouseSourceSplit(
                        "sql-1",
                        path,
                        "node-1:8123",
                        ClickHouseSourceSplit.Mode.SQL_QUERY,
                        Collections.<String>emptyList(),
                        "select id from analytics.orders;");

        String sql = ClickHouseSourceSqlBuilder.build(split, config, table);

        assertEquals(
                "SELECT * FROM (select id from analytics.orders) AS _link_up_source WHERE (id < 1000)",
                sql);
    }

    @Test
    public void escapesPartLiteral() {
        assertTrue(ClickHouseSourceSqlBuilder.quoteLiteral("a'b").contains("\\'"));
    }
}
