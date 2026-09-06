package com.link.up.connector.mongodb.config;

import org.junit.Assert;
import org.junit.Test;

public class MongoCatalogConfigTest {

    @Test
    public void parsesDefaultDatabaseFromUri() {
        MongoCatalogConfig config = MongoCatalogConfig.of(
                "mongodb://user:password@localhost:27017/business?authSource=admin");

        Assert.assertEquals("business", config.getDefaultDatabase());
        Assert.assertEquals(MongoCatalogConfig.DEFAULT_SCHEMA_SAMPLE_SIZE, config.getSchemaSampleSize());
        Assert.assertEquals(MongoCatalogConfig.DEFAULT_SCHEMA_MAX_DEPTH, config.getSchemaMaxDepth());
    }

    @Test
    public void allowsUriWithoutDefaultDatabase() {
        MongoCatalogConfig config = MongoCatalogConfig.of("mongodb://localhost:27017");
        Assert.assertNull(config.getDefaultDatabase());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsInvalidSampleSize() {
        new MongoCatalogConfig("mongodb://localhost:27017/test", 0, 8);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsExcessiveDepth() {
        new MongoCatalogConfig("mongodb://localhost:27017/test", 10, 33);
    }
}
