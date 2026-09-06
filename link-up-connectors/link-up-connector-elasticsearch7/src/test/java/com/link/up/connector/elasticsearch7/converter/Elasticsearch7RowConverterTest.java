package com.link.up.connector.elasticsearch7.converter;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Elasticsearch7RowConverterTest {

    @Test
    public void convertsDottedFieldsAndComplexJson() {
        TableSchema schema =
                TableSchema.builder()
                        .column(Column.builder("count", BasicType.INT_TYPE).build())
                        .column(Column.builder("customer.name", BasicType.STRING_TYPE).build())
                        .column(Column.builder("customer", BasicType.STRING_TYPE).build())
                        .build();

        Map<String, Object> customer = new LinkedHashMap<String, Object>();
        customer.put("name", "Ada");
        customer.put("tags", Arrays.asList("a", "b"));
        Map<String, Object> document = new LinkedHashMap<String, Object>();
        document.put("count", 7);
        document.put("customer", customer);

        FluxRow row = new Elasticsearch7RowConverter(schema).convert(document);
        assertEquals(7, row.getField(0));
        assertEquals("Ada", row.getField(1));
        assertEquals("{\"name\":\"Ada\",\"tags\":[\"a\",\"b\"]}", row.getField(2));
    }

    @Test
    public void refusesToGuessNumericArrayCardinality() {
        TableSchema schema =
                TableSchema.builder()
                        .column(Column.builder("count", BasicType.INT_TYPE).build())
                        .build();
        Map<String, Object> document = new LinkedHashMap<String, Object>();
        document.put("count", Arrays.asList(1, 2));
        try {
            new Elasticsearch7RowConverter(schema).convert(document);
            fail("Expected multi-valued numeric field to be rejected");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }
}
