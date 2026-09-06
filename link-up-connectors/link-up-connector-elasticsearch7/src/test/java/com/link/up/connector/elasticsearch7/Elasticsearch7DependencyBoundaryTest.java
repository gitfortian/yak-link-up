package com.link.up.connector.elasticsearch7;

import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Elasticsearch7DependencyBoundaryTest {

    @Test
    public void moduleOwnsElasticsearch7SdkOnly() {
        assertEquals("elasticsearch7", Elasticsearch7ConnectorIdentity.IDENTIFIER);
        assertEquals(7, Elasticsearch7ConnectorIdentity.MAJOR_VERSION);
        assertEquals(
                "org.elasticsearch.client.RestHighLevelClient",
                RestHighLevelClient.class.getName());
        assertClassNotPresent("co.elastic.clients.elasticsearch.ElasticsearchClient");
    }

    private static void assertClassNotPresent(String className) {
        try {
            Class.forName(className);
            fail("Elasticsearch 8 SDK leaked into Elasticsearch 7 module: " + className);
        } catch (ClassNotFoundException expected) {
            // Expected: ES7 and ES8 SDK classpaths stay separate at module level.
        }
    }
}
