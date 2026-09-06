package com.link.up.connector.elasticsearch8;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Elasticsearch8DependencyBoundaryTest {

    @Test
    public void moduleOwnsElasticsearch8SdkAndJackson2MapperOnly() {
        assertEquals("elasticsearch8", Elasticsearch8ConnectorIdentity.IDENTIFIER);
        assertEquals(8, Elasticsearch8ConnectorIdentity.MAJOR_VERSION);
        assertEquals(
                "co.elastic.clients.elasticsearch.ElasticsearchClient",
                ElasticsearchClient.class.getName());
        assertEquals(
                "co.elastic.clients.json.jackson.JacksonJsonpMapper",
                JacksonJsonpMapper.class.getName());
        assertClassNotPresent("org.elasticsearch.client.RestHighLevelClient");
        assertClassNotPresent("tools.jackson.databind.ObjectMapper");
    }

    private static void assertClassNotPresent(String className) {
        try {
            Class.forName(className);
            fail("Unexpected dependency leaked into Elasticsearch 8 module: " + className);
        } catch (ClassNotFoundException expected) {
            // Expected by the version / Java 8 dependency boundary.
        }
    }
}
