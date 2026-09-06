package com.link.up.connector.elasticsearch7.source;

import com.link.up.api.connector.schema.ConnectorCapability;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Elasticsearch7SourceFactoryTest {

    @Test
    public void exposesVersionedBoundedSourceIdentityOnly() {
        Elasticsearch7SourceFactory factory = new Elasticsearch7SourceFactory();
        assertEquals("elasticsearch7", factory.factoryIdentifier());
        assertTrue(factory.capabilities().contains(ConnectorCapability.TABLE_SCHEMA_DISCOVERY));
        assertTrue(factory.capabilities().contains(ConnectorCapability.PARTITION_SPLIT));
        assertFalse(factory.capabilities().contains(ConnectorCapability.MULTI_TABLE));
    }
}
