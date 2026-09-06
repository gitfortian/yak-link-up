package com.link.up.connector.elasticsearch8.source;

import com.link.up.api.connector.schema.ConnectorCapability;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Elasticsearch8SourceFactoryTest {

    @Test
    public void exposesOnlyBoundedSourceCapabilities() {
        Elasticsearch8SourceFactory factory = new Elasticsearch8SourceFactory();
        assertEquals("elasticsearch8", factory.factoryIdentifier());
        assertTrue(factory.capabilities().contains(ConnectorCapability.TABLE_SCHEMA_DISCOVERY));
        assertTrue(factory.capabilities().contains(ConnectorCapability.PARTITION_SPLIT));
        assertFalse(factory.capabilities().contains(ConnectorCapability.MULTI_TABLE));
        assertFalse(factory.capabilities().contains(ConnectorCapability.UPSERT));
    }
}
