package com.link.up.connector.elasticsearch8.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Elasticsearch8SinkPreparerTest {

    @Test
    public void acceptsSafeIntegerWideningAndReordersTarget() {
        CatalogTable source = table(
                Column.builder("id", BasicType.STRING_TYPE).sourceType("varchar").build(),
                Column.builder("amount", BasicType.INT_TYPE).sourceType("int").build());
        CatalogTable target = table(
                Column.builder("amount", BasicType.LONG_TYPE).sourceType("long").build(),
                Column.builder("id", BasicType.STRING_TYPE).sourceType("keyword").build());

        CatalogTable prepared = Elasticsearch8SinkPreparer.validateAndReorder(
                source, target, "id");
        assertEquals("id", prepared.getTableSchema().getColumn(0).getName());
        assertEquals("amount", prepared.getTableSchema().getColumn(1).getName());
    }

    @Test
    public void rejectsNumericNarrowing() {
        CatalogTable source = table(
                Column.builder("amount", BasicType.LONG_TYPE).sourceType("bigint").build());
        CatalogTable target = table(
                Column.builder("amount", BasicType.INT_TYPE).sourceType("integer").build());
        try {
            Elasticsearch8SinkPreparer.validateAndReorder(source, target, null);
            fail("Expected narrowing rejection");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    private static CatalogTable table(Column... columns) {
        TableSchema.Builder schema = TableSchema.builder();
        for (Column column : columns) {
            schema.column(column);
        }
        return CatalogTable.builder(TablePath.of("orders"), schema.build()).build();
    }
}
