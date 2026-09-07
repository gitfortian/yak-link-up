package com.link.up.connector.jdbc.catalog.gbase.gbase8c;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.WritableCatalog;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GBase8cCatalogTest {

    @Test
    public void catalogExposesWritableLifecycleForSafeMissingTableCreation() {
        assertTrue(catalog() instanceof WritableCatalog);
    }

    @Test
    public void automaticTargetStageStillRejectsDestructiveSchemaRecreation() {
        GBase8cCatalog catalog = catalog();

        try {
            catalog.dropTable(
                    TablePath.of("app", "public", "orders"),
                    false);
            fail("GBase 8c automatic-target Sink must not drop target tables");
        } catch (CatalogException expected) {
            assertTrue(expected.getMessage().contains("drop table"));
            assertTrue(expected.getMessage().contains("remains disabled"));
        }
    }

    @Test
    public void automaticTargetStageStillRejectsSchemaEvolution() {
        GBase8cCatalog catalog = catalog();

        try {
            catalog.addColumn(
                    TablePath.of("app", "public", "orders"),
                    Column.builder("extra", BasicType.STRING_TYPE).build());
            fail("GBase 8c automatic-target Sink must not add target columns");
        } catch (CatalogException expected) {
            assertTrue(expected.getMessage().contains("add column"));
            assertTrue(expected.getMessage().contains("remains disabled"));
        }
    }

    @Test
    public void automaticTargetStageStillRejectsDatabaseDdl() {
        GBase8cCatalog catalog = catalog();

        try {
            catalog.createDatabase("archive", false);
            fail("GBase 8c automatic-target Sink must not create databases");
        } catch (CatalogException expected) {
            assertTrue(expected.getMessage().contains("create database"));
        }
    }

    private static GBase8cCatalog catalog() {
        return new GBase8cCatalog(
                "gbase8c",
                new JdbcCatalogConfig(
                        "jdbc:gbase8c://127.0.0.1:5432/app",
                        "gbase",
                        "password",
                        "com.gbase8c.Driver",
                        Collections.emptyMap(),
                        false),
                "public");
    }
}
