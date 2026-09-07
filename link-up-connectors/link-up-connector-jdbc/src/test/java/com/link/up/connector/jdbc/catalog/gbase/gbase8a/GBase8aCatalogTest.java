package com.link.up.connector.jdbc.catalog.gbase.gbase8a;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GBase8aCatalogTest {

    @Test
    public void automaticTargetStageStillRejectsDestructiveDropBeforeRecreate() {
        GBase8aCatalog catalog = catalog();
        try {
            catalog.dropTable(TablePath.of("target_db", "orders"), false);
            fail("GBase 8a automatic-target stage must not drop target tables");
        } catch (CatalogException expected) {
            assertTrue(expected.getMessage().contains("drop table"));
        }
    }

    @Test
    public void automaticTargetStageStillRejectsRuntimeAddColumn() {
        GBase8aCatalog catalog = catalog();
        try {
            catalog.addColumn(
                    TablePath.of("target_db", "orders"),
                    Column.builder("new_col", BasicType.STRING_TYPE).build());
            fail("GBase 8a automatic-target stage must not mutate existing target schema");
        } catch (CatalogException expected) {
            assertTrue(expected.getMessage().contains("add column"));
        }
    }

    @Test
    public void automaticTargetStageStillRejectsDatabaseDdl() {
        GBase8aCatalog catalog = catalog();
        try {
            catalog.createDatabase("archive", false);
            fail("GBase 8a automatic-target stage must not create databases");
        } catch (CatalogException expected) {
            assertTrue(expected.getMessage().contains("create database"));
        }

        try {
            catalog.dropDatabase("archive", false);
            fail("GBase 8a automatic-target stage must not drop databases");
        } catch (CatalogException expected) {
            assertTrue(expected.getMessage().contains("drop database"));
        }
    }

    private static GBase8aCatalog catalog() {
        return new GBase8aCatalog(
                "gbase8a",
                new JdbcCatalogConfig(
                        "jdbc:gbase://127.0.0.1:5258/target_db",
                        "gbase",
                        "password",
                        "com.gbase.jdbc.Driver",
                        Collections.emptyMap(),
                        false),
                "target_db");
    }
}
