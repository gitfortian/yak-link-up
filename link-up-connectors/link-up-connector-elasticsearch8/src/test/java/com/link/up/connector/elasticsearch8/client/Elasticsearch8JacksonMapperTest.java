package com.link.up.connector.elasticsearch8.client;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.stream.JsonParser;
import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.assertTrue;

/** Verifies Stage 3 can parse Query DSL with the Jackson 2 mapper path on the Java 8 baseline. */
public class Elasticsearch8JacksonMapperTest {

    @Test
    public void jackson2MapperParsesQueryDsl() {
        JacksonJsonpMapper mapper = new JacksonJsonpMapper(new ObjectMapper());
        JsonParser parser = mapper.jsonProvider().createParser(
                new StringReader("{\"range\":{\"created_at\":{\"gte\":\"2026-01-01\"}}}"));
        try {
            Query query = Query._DESERIALIZER.deserialize(parser, mapper);
            assertTrue(query.isRange());
        } finally {
            parser.close();
        }
    }
}
