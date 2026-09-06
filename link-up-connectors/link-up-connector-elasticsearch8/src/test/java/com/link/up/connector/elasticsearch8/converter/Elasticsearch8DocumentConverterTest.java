package com.link.up.connector.elasticsearch8.converter;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Elasticsearch8DocumentConverterTest {

    @Test
    @SuppressWarnings("unchecked")
    public void rebuildsDottedPathsAndUsesStableDocumentId() {
        CatalogTable source = table(
                Column.builder("order_id", BasicType.STRING_TYPE).sourceType("varchar").build(),
                Column.builder("customer.name", BasicType.STRING_TYPE).sourceType("varchar").build());
        CatalogTable target = table(
                Column.builder("order_id", BasicType.STRING_TYPE).sourceType("keyword").build(),
                Column.builder("customer.name", BasicType.STRING_TYPE).sourceType("keyword").build());

        Elasticsearch8DocumentConverter.Document document =
                new Elasticsearch8DocumentConverter(source, target, "order_id")
                        .convert(FluxRow.of("o-1", "Ada"));

        assertEquals("o-1", document.getId());
        Map<String, Object> customer =
                (Map<String, Object>) document.getSource().get("customer");
        assertEquals("Ada", customer.get("name"));
    }

    @Test
    public void parsesNewEs8StructuredJsonBoundaries() {
        CatalogTable source = table(
                Column.builder("metrics", BasicType.STRING_TYPE).sourceType("text").build());
        CatalogTable target = table(
                Column.builder("metrics", BasicType.STRING_TYPE)
                        .sourceType("aggregate_metric_double")
                        .build());
        Object metrics = new Elasticsearch8DocumentConverter(source, target, null)
                .convert(FluxRow.of("{\"min\":1.0,\"max\":2.0,\"sum\":3.0,\"value_count\":2}"))
                .getSource().get("metrics");
        assertTrue(metrics instanceof Map);
    }

    @Test
    public void rejectsOverlappingParentChildPaths() {
        CatalogTable source = table(
                Column.builder("customer", BasicType.STRING_TYPE).sourceType("text").build(),
                Column.builder("customer.name", BasicType.STRING_TYPE).sourceType("text").build());
        CatalogTable target = table(
                Column.builder("customer", BasicType.STRING_TYPE).sourceType("object").build(),
                Column.builder("customer.name", BasicType.STRING_TYPE).sourceType("keyword").build());
        try {
            new Elasticsearch8DocumentConverter(source, target, null);
            fail("Expected overlapping path rejection");
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
