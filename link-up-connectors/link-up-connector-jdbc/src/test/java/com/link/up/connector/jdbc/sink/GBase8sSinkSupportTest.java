package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8s.GBase8sDialect;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GBase8sSinkSupportTest {

    @Test
    public void implicitTargetDropsSourceDatabaseAndOwnerMetadata() {
        TablePath target = GBase8sSinkSupport.resolveImplicitTargetPath(
                config("targetdb", "sinkowner", DatabaseIdentifier.GBASE8S),
                TablePath.of("source_db", "public", "orders"));

        assertEquals(
                TablePath.of("targetdb", "sinkowner", "orders"),
                target);
    }

    @Test
    public void usernameIsDefaultOwnerWhenSchemaIsNotConfigured() {
        TablePath target = GBase8sSinkSupport.resolveImplicitTargetPath(
                config("targetdb", null, DatabaseIdentifier.GBASE8S),
                TablePath.of("source_db", "public", "orders"));

        assertEquals(
                TablePath.of("targetdb", "gbasedbt", "orders"),
                target);
    }

    @Test
    public void explicitOwnerMappingWinsOverConfiguredDefaultOwner() {
        TablePath target = GBase8sSinkSupport.resolveExplicitTargetPath(
                config("targetdb", "sinkowner", DatabaseIdentifier.GBASE8S),
                TablePath.of(null, "archive_owner", "orders"));

        assertEquals(
                TablePath.of("targetdb", "archive_owner", "orders"),
                target);
    }

    @Test
    public void explicitSameDatabaseOwnerMappingIsAccepted() {
        TablePath target = GBase8sSinkSupport.resolveExplicitTargetPath(
                config("targetdb", "sinkowner", DatabaseIdentifier.GBASE8S),
                TablePath.of("targetdb", "archive_owner", "orders"));

        assertEquals(
                TablePath.of("targetdb", "archive_owner", "orders"),
                target);
    }

    @Test
    public void explicitCrossDatabaseMappingIsRejected() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GBase8sSinkSupport.resolveExplicitTargetPath(
                        config("targetdb", "sinkowner", DatabaseIdentifier.GBASE8S),
                        TablePath.of("otherdb", "archive_owner", "orders")));

        assertTrue(error.getMessage().contains("must match Sink JDBC URL database"));
    }

    @Test
    public void dedicatedUrlCanSelectSinkSupportWithoutExplicitDialect() {
        assertTrue(GBase8sSinkSupport.accepts(config("targetdb", null, null)));
        assertTrue(GBase8sSinkSupport.accepts(
                config("targetdb", null, DatabaseIdentifier.GBASE8S)));
        assertFalse(GBase8sSinkSupport.accepts(
                config("targetdb", null, DatabaseIdentifier.GBASE8A)));
    }

    @Test
    public void existingTableSinkDoesNotGenerateAutomaticCreateTableSql() {
        JdbcConnectionConfig config = config(
                "targetdb",
                "sinkowner",
                DatabaseIdentifier.GBASE8S);

        assertNull(
                JdbcCreateTableSqlResolver.resolve(
                        new GBase8sDialect(config),
                        config,
                        table()));
    }

    private static CatalogTable table() {
        TableSchema schema = TableSchema.builder()
                .columns(Collections.singletonList(
                        Column.builder("id", BasicType.INT_TYPE)
                                .nullable(false)
                                .build()))
                .build();

        return CatalogTable.builder(
                        TablePath.of("source_db", "public", "orders"),
                        schema)
                .build();
    }

    private static JdbcConnectionConfig config(
            String database,
            String schema,
            String dialect) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put(
                "url",
                "jdbc:gbasedbt-sqli://127.0.0.1:9088/"
                        + database
                        + ":GBASEDBTSERVER=gbase01;IFX_LOCK_MODE_WAIT=10");
        values.put("driver", "com.gbasedbt.jdbc.Driver");
        values.put("username", "gbasedbt");
        if (schema != null) {
            values.put("schema", schema);
        }
        if (dialect != null) {
            values.put("dialect", dialect);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }
}
