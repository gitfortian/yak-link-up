package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.goldendb.GoldenDbDialect;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GoldenDbSinkSupportTest {

    @Test
    public void sourceDatabaseDoesNotLeakIntoGoldenDbTarget() {
        TablePath target =
                GoldenDbSinkSupport.resolveTargetPath(
                        config(
                                "jdbc:mysql://127.0.0.1:3306/target_db",
                                null),
                        TablePath.of(
                                "source_db",
                                "orders"));

        assertEquals(
                TablePath.of(
                        "target_db",
                        "orders"),
                target);
    }

    @Test
    public void configuredSchemaActsAsTargetDatabaseWhenUrlIsUnqualified() {
        TablePath target =
                GoldenDbSinkSupport.resolveTargetPath(
                        config(
                                "jdbc:mysql://127.0.0.1:3306",
                                "configured_db"),
                        TablePath.of(
                                "source_db",
                                "orders"));

        assertEquals(
                TablePath.of(
                        "configured_db",
                        "orders"),
                target);
    }

    @Test
    public void stageOneDoesNotGenerateAutomaticCreateTableSql() {
        JdbcConnectionConfig config =
                config(
                        "jdbc:mysql://127.0.0.1:3306/target_db",
                        null);

        assertNull(
                JdbcCreateTableSqlResolver.resolve(
                        new GoldenDbDialect(config),
                        config,
                        table()));
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
                                "source_db",
                                "orders"),
                        schema)
                .build();
    }

    private static JdbcConnectionConfig config(
            String url,
            String schema) {

        Map<String, Object> values =
                new LinkedHashMap<String, Object>();

        values.put("url", url);
        values.put(
                "driver",
                "com.mysql.cj.jdbc.Driver");
        values.put(
                "dialect",
                DatabaseIdentifier.GOLDENDB);

        if (schema != null) {
            values.put("schema", schema);
        }

        return JdbcConnectionConfig.of(
                ReadonlyConfig.fromMap(values));
    }
}
