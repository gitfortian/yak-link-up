package com.link.up.connector.jdbc.catalog.gbase.gbase8s;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.WritableCatalog;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GBase8sCatalogTest {

    @Test
    public void existingTableSinkCatalogStaysBoundToOneDatabase() {
        GBase8sCatalog catalog = new GBase8sCatalog(
                "gbase8s",
                config(),
                "testdb",
                "gbasedbt");

        assertEquals("testdb", catalog.getDefaultDatabase().get());
        assertTrue(catalog instanceof WritableCatalog);
    }

    @Test
    public void existingTableSinkRejectsStructureChangingDdl() {
        GBase8sCatalog catalog = new GBase8sCatalog(
                "gbase8s",
                config(),
                "testdb",
                "gbasedbt");

        assertBlocked(() -> catalog.createDatabase("archive", false), "create database");
        assertBlocked(() -> catalog.dropDatabase("archive", false), "drop database");
        assertBlocked(() -> catalog.createTable(table(), false), "create table");
        assertBlocked(
                () -> catalog.addColumn(
                        TablePath.of("testdb", "gbasedbt", "orders"),
                        Column.builder("extra", BasicType.STRING_TYPE).build()),
                "add column");
        assertBlocked(
                () -> catalog.dropTable(
                        TablePath.of("testdb", "gbasedbt", "orders"),
                        false),
                "drop table");
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

    private static CatalogTable table() {
        TableSchema schema = TableSchema.builder()
                .columns(Collections.singletonList(
                        Column.builder("id", BasicType.INT_TYPE)
                                .nullable(false)
                                .build()))
                .build();
        return CatalogTable.builder(
                        TablePath.of("testdb", "gbasedbt", "orders"),
                        schema)
                .build();
    }

    private static void assertBlocked(
            Runnable operation,
            String operationName) {
        CatalogException error = assertThrows(CatalogException.class, operation::run);
        assertTrue(error.getMessage().contains(operationName));
        assertTrue(error.getMessage().contains("pre-create"));
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
