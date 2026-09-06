package com.link.up.connector.clickhouse.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClickHouseSinkPreparerTest {

    @Test
    public void reordersTargetColumnsToSourceOrderAndAllowsIntegerWidening() {
        CatalogTable source =
                CatalogTable.builder(
                                TablePath.of("src", "orders"),
                                TableSchema.builder()
                                        .column(Column.builder("id", BasicType.INT_TYPE).nullable(false).build())
                                        .column(Column.builder("name", BasicType.STRING_TYPE).build())
                                        .build())
                        .build();

        CatalogTable target =
                CatalogTable.builder(
                                TablePath.of("analytics", "orders"),
                                TableSchema.builder()
                                        .column(Column.builder("name", BasicType.STRING_TYPE).sourceType("String").build())
                                        .column(Column.builder("id", BasicType.LONG_TYPE).nullable(false).sourceType("Int64").build())
                                        .build())
                        .build();

        CatalogTable prepared = ClickHouseSinkPreparer.validateAndReorder(source, target);

        assertEquals("id", prepared.getTableSchema().getColumn(0).getName());
        assertEquals("name", prepared.getTableSchema().getColumn(1).getName());
        assertEquals("analytics.orders", prepared.getTablePath().toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNullableSourceIntoNonNullableTarget() {
        CatalogTable source =
                CatalogTable.builder(
                                TablePath.of("src", "orders"),
                                TableSchema.builder()
                                        .column(Column.builder("id", BasicType.LONG_TYPE).nullable(true).build())
                                        .build())
                        .build();
        CatalogTable target =
                CatalogTable.builder(
                                TablePath.of("analytics", "orders"),
                                TableSchema.builder()
                                        .column(Column.builder("id", BasicType.LONG_TYPE).nullable(false).sourceType("Int64").build())
                                        .build())
                        .build();

        ClickHouseSinkPreparer.validateAndReorder(source, target);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsUnsupportedBinarySource() {
        CatalogTable source =
                CatalogTable.builder(
                                TablePath.of("src", "orders"),
                                TableSchema.builder()
                                        .column(Column.builder("payload", BasicType.BYTES_TYPE).build())
                                        .build())
                        .build();
        CatalogTable target =
                CatalogTable.builder(
                                TablePath.of("analytics", "orders"),
                                TableSchema.builder()
                                        .column(Column.builder("payload", BasicType.STRING_TYPE).sourceType("String").build())
                                        .build())
                        .build();

        ClickHouseSinkPreparer.validateAndReorder(source, target);
    }
}
