package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8a.GBase8aDialect;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GBase8aSinkSupportTest {

    @Test
    public void sourceDatabaseAndSchemaDoNotLeakIntoTarget() {
        TablePath target = GBase8aSinkSupport.resolveTargetPath(
                config("target_db", DatabaseIdentifier.GBASE8A),
                TablePath.of("source_db", "source_schema", "orders"));

        assertEquals(
                TablePath.of("target_db", "orders"),
                target);
    }

    @Test
    public void unqualifiedTargetUsesJdbcUrlDatabase() {
        TablePath target = GBase8aSinkSupport.resolveTargetPath(
                config("target_db", DatabaseIdentifier.GBASE8A),
                TablePath.of("orders"));

        assertEquals(TablePath.of("target_db", "orders"), target);
    }

    @Test
    public void explicitTargetMayRepeatUrlDatabase() {
        JdbcConnectionConfig config = config("target_db", DatabaseIdentifier.GBASE8A);
        GBase8aSinkSupport.validateExplicitTargetPath(
                config,
                TablePath.of("target_db", "orders"));

        assertEquals(
                TablePath.of("target_db", "orders"),
                GBase8aSinkSupport.resolveTargetPath(
                        config,
                        TablePath.of("target_db", "orders")));
    }

    @Test
    public void explicitCrossDatabaseTargetFailsClearly() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GBase8aSinkSupport.validateExplicitTargetPath(
                        config("target_db", DatabaseIdentifier.GBASE8A),
                        TablePath.of("archive", "orders")));

        assertTrue(error.getMessage().contains("must match Sink JDBC URL database"));
    }

    @Test
    public void dedicatedUrlCanSelectSinkSupportWithoutExplicitDialect() {
        assertTrue(GBase8aSinkSupport.accepts(config("target_db", null)));
        assertTrue(GBase8aSinkSupport.accepts(
                config("target_db", DatabaseIdentifier.GBASE8A)));
        assertFalse(GBase8aSinkSupport.accepts(
                config("target_db", DatabaseIdentifier.POSTGRESQL)));
    }

    @Test
    public void existingTableSinkDoesNotGenerateAutomaticCreateTableSql() {
        JdbcConnectionConfig config = config("target_db", DatabaseIdentifier.GBASE8A);
        assertNull(
                JdbcCreateTableSqlResolver.resolve(
                        new GBase8aDialect(config),
                        config,
                        table()));
    }

    private static CatalogTable table() {
        TableSchema schema = TableSchema.builder()
                .columns(Collections.singletonList(
                        Column.builder("id", BasicType.LONG_TYPE)
                                .nullable(false)
                                .build()))
                .build();
        return CatalogTable.builder(
                        TablePath.of("source_db", "orders"),
                        schema)
                .build();
    }

    private static JdbcConnectionConfig config(
            String database,
            String dialect) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", "jdbc:gbase://127.0.0.1:5258/" + database);
        values.put("driver", "com.gbase.jdbc.Driver");
        values.put("username", "gbase");
        if (dialect != null) {
            values.put("dialect", dialect);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }
}
