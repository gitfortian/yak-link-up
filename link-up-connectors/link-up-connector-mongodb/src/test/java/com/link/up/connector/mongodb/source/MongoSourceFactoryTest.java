package com.link.up.connector.mongodb.source;

import com.link.up.api.connector.schema.ConnectorCapability;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongoSourceFactoryTest {

    @Test
    public void exposesOnlyCapabilitiesImplementedInStage2() {
        MongoSourceFactory factory = new MongoSourceFactory();

        assertEquals("mongodb", factory.factoryIdentifier());
        assertTrue(factory.capabilities().contains(ConnectorCapability.TABLE_SCHEMA_DISCOVERY));
        assertFalse(factory.capabilities().contains(ConnectorCapability.PARTITION_SPLIT));
        assertFalse(factory.capabilities().contains(ConnectorCapability.MULTI_TABLE));
    }
}
