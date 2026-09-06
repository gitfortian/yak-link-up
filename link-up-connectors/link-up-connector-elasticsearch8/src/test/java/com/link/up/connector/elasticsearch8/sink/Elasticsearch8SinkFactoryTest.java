package com.link.up.connector.elasticsearch8.sink;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Elasticsearch8SinkFactoryTest {

    @Test
    public void exposesVersionedIdentifierWithoutRealtimeCapabilities() {
        Elasticsearch8SinkFactory factory = new Elasticsearch8SinkFactory();
        assertEquals("elasticsearch8", factory.factoryIdentifier());
        assertTrue(factory.capabilities().isEmpty());
    }
}
