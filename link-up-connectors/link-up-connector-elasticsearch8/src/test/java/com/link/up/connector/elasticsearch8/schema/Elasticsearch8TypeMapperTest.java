package com.link.up.connector.elasticsearch8.schema;

import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import jakarta.json.stream.JsonParser;
import org.junit.Test;

import java.io.StringReader;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Elasticsearch8TypeMapperTest {

    @Test
    public void mapsStableScalarsAndProjectedNestedFields() {
        Map<String, Object> properties = new LinkedHashMap<String, Object>();
        properties.put("active", field("boolean"));
        properties.put("count", field("long"));
        properties.put("unsigned", field("unsigned_long"));

        Map<String, Object> customerProperties = new LinkedHashMap<String, Object>();
        customerProperties.put("name", field("keyword"));
        Map<String, Object> customer = new LinkedHashMap<String, Object>();
        customer.put("properties", customerProperties);
        properties.put("customer", customer);

        Map<String, Object> mapping = new LinkedHashMap<String, Object>();
        mapping.put("properties", properties);

        TableSchema schema = Elasticsearch8TypeMapper.toTableSchema(
                mapping,
                Arrays.asList("active", "count", "unsigned", "customer.name"));

        assertEquals(BasicType.BOOLEAN_TYPE, schema.getColumn("active").getDataType());
        assertEquals(BasicType.LONG_TYPE, schema.getColumn("count").getDataType());
        assertEquals(new DecimalType(20, 0), schema.getColumn("unsigned").getDataType());
        assertEquals(BasicType.STRING_TYPE, schema.getColumn("customer.name").getDataType());
    }

    @Test
    public void mapsTypedJavaClientMappingThroughPublicBoundary() {
        JacksonJsonpMapper mapper = new JacksonJsonpMapper(new ObjectMapper());
        JsonParser parser = mapper.jsonProvider().createParser(new StringReader(
                "{\"properties\":{\"count\":{\"type\":\"long\"},\"name\":{\"type\":\"keyword\"}}}"));
        final TypeMapping mapping;
        try {
            mapping = TypeMapping._DESERIALIZER.deserialize(parser, mapper);
        } finally {
            parser.close();
        }

        TableSchema schema = Elasticsearch8TypeMapper.toTableSchema(
                mapping,
                Arrays.asList("count", "name"));
        assertEquals(BasicType.LONG_TYPE, schema.getColumn("count").getDataType());
        assertEquals(BasicType.STRING_TYPE, schema.getColumn("name").getDataType());
    }

    private static Map<String, Object> field(String type) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("type", type);
        return result;
    }
}
