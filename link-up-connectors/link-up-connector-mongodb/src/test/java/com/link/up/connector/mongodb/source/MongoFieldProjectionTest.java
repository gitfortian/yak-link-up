package com.link.up.connector.mongodb.source;

import org.bson.BsonDocument;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongoFieldProjectionTest {

    @Test
    public void parentSelectionSuppressesChildProjectionCollision() {
        BsonDocument projection = MongoFieldProjection.build(
                Arrays.asList("address.city", "address", "name"));

        assertEquals(1, projection.getInt32("address").getValue());
        assertEquals(1, projection.getInt32("name").getValue());
        assertFalse(projection.containsKey("address.city"));
        assertEquals(0, projection.getInt32("_id").getValue());
    }

    @Test
    public void selectedIdIsNotExcluded() {
        BsonDocument projection = MongoFieldProjection.build(
                Arrays.asList("_id", "address.city"));

        assertEquals(1, projection.getInt32("_id").getValue());
        assertEquals(1, projection.getInt32("address.city").getValue());
    }

    @Test
    public void emptyFieldListMeansNoProjection() {
        assertTrue(MongoFieldProjection.build(Collections.<String>emptyList()).isEmpty());
    }
}
