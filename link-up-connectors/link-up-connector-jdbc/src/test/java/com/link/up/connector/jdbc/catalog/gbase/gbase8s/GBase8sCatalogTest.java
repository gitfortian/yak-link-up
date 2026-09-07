package com.link.up.connector.jdbc.catalog.gbase.gbase8s;

import com.link.up.api.table.catalog.WritableCatalog;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class GBase8sCatalogTest {

    @Test
    public void sourceCatalogIsBoundToOneDatabaseAndRemainsReadOnly() {
        GBase8sCatalog catalog = new GBase8sCatalog(
                "gbase8s",
                config(),
                "testdb",
                "gbasedbt");

        assertEquals("testdb", catalog.getDefaultDatabase().get());
        assertFalse(catalog instanceof WritableCatalog);
    }

    @Test
    public void rejectsNonGBase8sJdbcUrl() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GBase8sCatalog(
                        "gbase8s",
                        new JdbcCatalogConfig(
                                "jdbc:gbase://127.0.0.1:5258/testdb",
                                "gbasedbt",
                                "password",
                                "com.gbasedbt.jdbc.Driver",
                                Collections.<String, String>emptyMap(),
                                false),
                        "testdb",
                        "gbasedbt"));
    }

    private static JdbcCatalogConfig config() {
        return new JdbcCatalogConfig(
                "jdbc:gbasedbt-sqli://127.0.0.1:9088/testdb:GBASEDBTSERVER=gbase01",
                "gbasedbt",
                "password",
                "com.gbasedbt.jdbc.Driver",
                Collections.<String, String>emptyMap(),
                false);
    }
}
