package com.link.up.connector.elasticsearch7.converter;

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

public class Elasticsearch7DocumentConverterTest {

    @Test
    @SuppressWarnings("unchecked")
    public void rebuildsDottedPathsAndUsesStableDocumentId() {
        CatalogTable source =
                table(
                        Column.builder("order_id", BasicType.STRING_TYPE).sourceType("varchar").build(),
                        Column.builder("customer.name", BasicType.STRING_TYPE).sourceType("varchar").build());
        CatalogTable target =
                table(
                        Column.builder("order_id", BasicType.STRING_TYPE).sourceType("keyword").build(),
                        Column.builder("customer.name", BasicType.STRING_TYPE).sourceType("keyword").build());

        Elasticsearch7DocumentConverter converter =
                new Elasticsearch7DocumentConverter(source, target, "order_id");
        Elasticsearch7DocumentConverter.Document document =
                converter.convert(FluxRow.of("o-1", "Ada"));

        assertEquals("o-1", document.getId());
        assertEquals("o-1", document.getSource().get("order_id"));
        Map<String, Object> customer = (Map<String, Object>) document.getSource().get("customer");
        assertEquals("Ada", customer.get("name"));
    }

    @Test
    public void parsesObjectJsonFromStringBoundary() {
        CatalogTable source =
                table(Column.builder("payload", BasicType.STRING_TYPE).sourceType("text").build());
        CatalogTable target =
                table(Column.builder("payload", BasicType.STRING_TYPE).sourceType("object").build());
        Elasticsearch7DocumentConverter converter =
                new Elasticsearch7DocumentConverter(source, target, null);

        Object payload = converter.convert(FluxRow.of("{\"x\":1}"))
                .getSource().get("payload");
        assertTrue(payload instanceof Map);
        assertEquals(1, ((Number) ((Map<?, ?>) payload).get("x")).intValue());
    }

    private static CatalogTable table(Column... columns) {
        TableSchema.Builder schema = TableSchema.builder();
        for (Column column : columns) {
            schema.column(column);
        }
        return CatalogTable.builder(TablePath.of("orders"), schema.build()).build();
    }
}
