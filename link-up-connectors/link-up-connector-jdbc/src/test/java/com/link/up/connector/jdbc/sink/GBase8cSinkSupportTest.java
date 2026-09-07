package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cCompatibilityMode;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GBase8cSinkSupportTest {

    @Test
    public void implicitTargetDropsSourceDatabaseAndSchemaMetadata() {
        TablePath target = GBase8cSinkSupport.resolveImplicitTargetPath(
                config("target_db", "landing", DatabaseIdentifier.GBASE8C),
                TablePath.of("source_db", "source_schema", "orders"));

        assertEquals(TablePath.of("target_db", "landing", "orders"), target);
    }

    @Test
    public void implicitTargetDoesNotLeakSchemaEvenWhenDatabaseNamesCoincide() {
        TablePath target = GBase8cSinkSupport.resolveImplicitTargetPath(
                config("target_db", "landing", DatabaseIdentifier.GBASE8C),
                TablePath.of("target_db", "source_schema", "orders"));

        assertEquals(TablePath.of("target_db", "landing", "orders"), target);
    }

    @Test
    public void implicitTargetFallsBackToPublicWithoutConfiguredSchema() {
        TablePath target = GBase8cSinkSupport.resolveImplicitTargetPath(
                config("target_db", null, DatabaseIdentifier.GBASE8C),
                TablePath.of("source_db", "source_schema", "orders"));

        assertEquals(TablePath.of("target_db", "public", "orders"), target);
    }

    @Test
    public void explicitSchemaTableMappingWinsOverConfiguredDefaultSchema() {
        TablePath target = GBase8cSinkSupport.resolveExplicitTargetPath(
                config("target_db", "public", DatabaseIdentifier.GBASE8C),
                TablePath.of(null, "archive", "orders"));

        assertEquals(TablePath.of("target_db", "archive", "orders"), target);
    }

    @Test
    public void explicitSameDatabaseSchemaMappingIsAccepted() {
        TablePath target = GBase8cSinkSupport.resolveExplicitTargetPath(
                config("target_db", "public", DatabaseIdentifier.GBASE8C),
                TablePath.of("target_db", "sales", "orders"));

        assertEquals(TablePath.of("target_db", "sales", "orders"), target);
    }

    @Test
    public void explicitCrossDatabaseMappingIsRejected() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GBase8cSinkSupport.resolveExplicitTargetPath(
                        config("target_db", "public", DatabaseIdentifier.GBASE8C),
                        TablePath.of("other_db", "sales", "orders")));

        assertTrue(error.getMessage().contains("must match Sink JDBC URL database"));
    }

    @Test
    public void dedicatedUrlCanSelectSinkSupportWithoutExplicitDialect() {
        assertTrue(GBase8cSinkSupport.accepts(config("target_db", null, null)));
        assertTrue(GBase8cSinkSupport.accepts(
                config("target_db", null, DatabaseIdentifier.GBASE8C)));
        assertFalse(GBase8cSinkSupport.accepts(
                config("target_db", null, DatabaseIdentifier.POSTGRESQL)));
    }

    @Test
    public void automaticCreatePreviewUsesResolvedModeAndTargetSchema() {
        JdbcConnectionConfig config =
                config("target_db", "landing", DatabaseIdentifier.GBASE8C);
        TablePath targetPath = GBase8cSinkSupport.resolveImplicitTargetPath(
                config,
                table().getTablePath());
        CatalogTable target = table().withPath(targetPath);

        String sql = GBase8cSinkSupport.resolveCreateTableSql(
                config,
                target,
                GBase8cCompatibilityMode.A);

        assertTrue(sql.startsWith("CREATE TABLE \"landing\".\"orders\" ("));
        assertTrue(sql.contains("\"id\" BIGINT NOT NULL"));
        assertTrue(sql.contains(
                "\"business_date\" TIMESTAMP(0) WITHOUT TIME ZONE NULL"));
        assertFalse(sql.contains("source_db"));
        assertFalse(sql.contains("source_schema"));
    }

    private static CatalogTable table() {
        TableSchema schema = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("id", BasicType.LONG_TYPE)
                                .nullable(false)
                                .build(),
                        Column.builder("business_date", BasicType.DATE_TYPE)
                                .nullable(true)
                                .build()))
                .build();

        return CatalogTable.builder(
                        TablePath.of("source_db", "source_schema", "orders"),
                        schema)
                .build();
    }

    private static JdbcConnectionConfig config(
            String database,
            String schema,
            String dialect) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", "jdbc:gbase8c://127.0.0.1:5432/" + database);
        values.put("driver", "com.gbase8c.Driver");
        values.put("username", "gbase");
        if (schema != null) {
            values.put("schema", schema);
        }
        if (dialect != null) {
            values.put("dialect", dialect);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }
}
