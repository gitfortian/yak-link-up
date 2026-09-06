package com.link.up.connector.jdbc.catalog.goldendb;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GoldenDbCatalogTest {

    @Test
    public void stageOneRejectsAutomaticTableCreation() {
        GoldenDbCatalog catalog =
                new GoldenDbCatalog(
                        "goldendb",
                        new JdbcCatalogConfig(
                                "jdbc:mysql://127.0.0.1:3306/target_db",
                                "user",
                                "password",
                                "com.mysql.cj.jdbc.Driver",
                                Collections.emptyMap(),
                                false));

        try {
            catalog.createTable(
                    table(),
                    false);
            fail("GoldenDB Stage 1 must not auto-create target tables");
        } catch (CatalogException expected) {
            assertTrue(
                    expected.getMessage()
                            .contains(
                                    "existing target tables only"));
        }
    }

    @Test
    public void stageOneRejectsDestructiveSchemaRecreationBeforeDrop() {
        GoldenDbCatalog catalog =
                new GoldenDbCatalog(
                        "goldendb",
                        new JdbcCatalogConfig(
                                "jdbc:mysql://127.0.0.1:3306/target_db",
                                "user",
                                "password",
                                "com.mysql.cj.jdbc.Driver",
                                Collections.emptyMap(),
                                false));

        try {
            catalog.dropTable(
                    TablePath.of(
                            "target_db",
                            "orders"),
                    false);
            fail("GoldenDB Stage 1 must not drop target tables");
        } catch (CatalogException expected) {
            assertTrue(
                    expected.getMessage()
                            .contains(
                                    "drop table"));
        }
    }

    private static CatalogTable table() {
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Collections.singletonList(
                                        Column.builder(
                                                        "id",
                                                        BasicType.LONG_TYPE)
                                                .nullable(false)
                                                .build()))
                        .build();

        return CatalogTable.builder(
                        TablePath.of(
                                "target_db",
                                "orders"),
                        schema)
                .build();
    }
}
