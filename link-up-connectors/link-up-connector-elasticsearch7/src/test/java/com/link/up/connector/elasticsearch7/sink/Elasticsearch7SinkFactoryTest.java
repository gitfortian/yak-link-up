package com.link.up.connector.elasticsearch7.sink;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Elasticsearch7SinkFactoryTest {

    @Test
    public void exposesVersionedIdentifierWithoutRealtimeCapabilities() {
        Elasticsearch7SinkFactory factory = new Elasticsearch7SinkFactory();
        assertEquals("elasticsearch7", factory.factoryIdentifier());
        assertTrue(factory.capabilities().isEmpty());
    }
}
