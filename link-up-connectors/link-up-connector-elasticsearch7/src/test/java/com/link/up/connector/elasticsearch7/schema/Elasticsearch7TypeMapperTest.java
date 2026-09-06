package com.link.up.connector.elasticsearch7.schema;

import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.SqlType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Elasticsearch7TypeMapperTest {

    @Test
    public void mapsScalarTypesAndKeepsComplexValuesAsStrings() {
        Map<String, Object> mapping = mapping();
        TableSchema schema =
                Elasticsearch7TypeMapper.toTableSchema(mapping, Collections.<String>emptyList());

        assertEquals(SqlType.STRING, schema.getColumn("id").getDataType().getSqlType());
        assertEquals(SqlType.INT, schema.getColumn("count").getDataType().getSqlType());
        assertEquals(SqlType.DOUBLE, schema.getColumn("price").getDataType().getSqlType());
        assertEquals(SqlType.STRING, schema.getColumn("customer").getDataType().getSqlType());
    }

    @Test
    public void resolvesProjectedDottedFieldFromNestedProperties() {
        TableSchema schema =
                Elasticsearch7TypeMapper.toTableSchema(
                        mapping(),
                        Arrays.asList("customer.name"));
        assertEquals(1, schema.getColumnCount());
        assertEquals("customer.name", schema.getColumn(0).getName());
        assertEquals(SqlType.STRING, schema.getColumn(0).getDataType().getSqlType());
    }

    private static Map<String, Object> mapping() {
        Map<String, Object> properties = new LinkedHashMap<String, Object>();
        properties.put("id", field("keyword"));
        properties.put("count", field("integer"));
        properties.put("price", field("scaled_float"));

        Map<String, Object> customerProperties = new LinkedHashMap<String, Object>();
        customerProperties.put("name", field("keyword"));
        Map<String, Object> customer = field("object");
        customer.put("properties", customerProperties);
        properties.put("customer", customer);

        Map<String, Object> mapping = new LinkedHashMap<String, Object>();
        mapping.put("properties", properties);
        return mapping;
    }

    private static Map<String, Object> field(String type) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("type", type);
        return result;
    }
}
