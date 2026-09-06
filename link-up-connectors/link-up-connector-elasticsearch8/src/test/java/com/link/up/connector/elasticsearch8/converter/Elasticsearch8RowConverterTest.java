package com.link.up.connector.elasticsearch8.converter;

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

public class Elasticsearch8RowConverterTest {

    @Test
    public void convertsNestedProjectionAndComplexJson() {
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("id", BasicType.STRING_TYPE).sourceType("keyword").build())
                .column(Column.builder("customer.name", BasicType.STRING_TYPE).sourceType("keyword").build())
                .column(Column.builder("payload", BasicType.STRING_TYPE).sourceType("object").build())
                .build();

        Map<String, Object> customer = new LinkedHashMap<String, Object>();
        customer.put("name", "Ada");
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("x", 1);
        Map<String, Object> source = new LinkedHashMap<String, Object>();
        source.put("id", "o-1");
        source.put("customer", customer);
        source.put("payload", payload);

        FluxRow row = new Elasticsearch8RowConverter(schema).convert(source);
        assertEquals("o-1", row.getField(0));
        assertEquals("Ada", row.getField(1));
        assertEquals("{\"x\":1}", row.getField(2));
    }

    @Test
    public void rejectsTypedMultiValueWhenMappingCannotDeclareArrayCardinality() {
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("count", BasicType.LONG_TYPE).sourceType("long").build())
                .build();
        Map<String, Object> source = new LinkedHashMap<String, Object>();
        source.put("count", Arrays.asList(1, 2));
        try {
            new Elasticsearch8RowConverter(schema).convert(source);
            fail("Expected typed multi-value rejection");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }
}
