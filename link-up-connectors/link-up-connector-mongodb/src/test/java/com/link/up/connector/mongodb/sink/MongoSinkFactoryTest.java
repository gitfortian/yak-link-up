package com.link.up.connector.mongodb.sink;

import com.link.up.api.connector.schema.ConnectorCapability;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongoSinkFactoryTest {

    @Test
    public void exposesInsertOnlyBoundedSinkBoundary() {
        MongoSinkFactory factory = new MongoSinkFactory();

        assertEquals("mongodb", factory.factoryIdentifier());
        assertTrue(factory.capabilities().isEmpty());
        assertFalse(factory.capabilities().contains(ConnectorCapability.UPSERT));
        assertFalse(factory.capabilities().contains(ConnectorCapability.AUTO_CREATE_TABLE));
        assertFalse(factory.capabilities().contains(ConnectorCapability.MULTI_TABLE));
    }
}
