package com.link.up.connector.elasticsearch7.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Elasticsearch7SinkPreparerTest {

    @Test
    public void allowsSafeIntegerWidening() {
        CatalogTable source = table(Column.builder("count", BasicType.INT_TYPE).build());
        CatalogTable target = table(Column.builder("count", BasicType.LONG_TYPE).sourceType("long").build());
        CatalogTable prepared =
                Elasticsearch7SinkPreparer.validateAndReorder(source, target, null);
        assertEquals("long", prepared.getTableSchema().getColumn(0).getSourceType());
    }

    @Test
    public void rejectsNumericNarrowing() {
        CatalogTable source = table(Column.builder("count", BasicType.LONG_TYPE).build());
        CatalogTable target = table(Column.builder("count", BasicType.INT_TYPE).sourceType("integer").build());
        try {
            Elasticsearch7SinkPreparer.validateAndReorder(source, target, null);
            fail("Expected safe mapping validation failure");
        } catch (IllegalArgumentException expected) {
            org.junit.Assert.assertTrue(expected.getMessage().contains("safe bounded-sink"));
        }
    }

    @Test
    public void requiresConfiguredDocumentIdFieldInSourceSchema() {
        CatalogTable source = table(Column.builder("name", BasicType.STRING_TYPE).build());
        CatalogTable target = table(Column.builder("name", BasicType.STRING_TYPE).sourceType("keyword").build());
        try {
            Elasticsearch7SinkPreparer.validateAndReorder(source, target, "id");
            fail("Expected document id field validation failure");
        } catch (IllegalArgumentException expected) {
            org.junit.Assert.assertTrue(expected.getMessage().contains("document_id_field"));
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
