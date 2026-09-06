package com.link.up.connector.elasticsearch8;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Elasticsearch8DependencyBoundaryTest {

    @Test
    public void moduleOwnsElasticsearch8SdkOnly() {
        assertEquals("elasticsearch8", Elasticsearch8ConnectorIdentity.IDENTIFIER);
        assertEquals(8, Elasticsearch8ConnectorIdentity.MAJOR_VERSION);
        assertEquals(
                "co.elastic.clients.elasticsearch.ElasticsearchClient",
                ElasticsearchClient.class.getName());
        assertClassNotPresent("org.elasticsearch.client.RestHighLevelClient");
    }

    private static void assertClassNotPresent(String className) {
        try {
            Class.forName(className);
            fail("Elasticsearch 7 SDK leaked into Elasticsearch 8 module: " + className);
        } catch (ClassNotFoundException expected) {
            // Expected: ES8 and ES7 SDK classpaths stay separate at module level.
        }
    }
}
