package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.gbase.gbase8c.GBase8cCreateTableSqlBuilder;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cCompatibilityMode;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cJdbcUrl;

/** Target-path and compatibility-aware automatic-DDL support for the GBase 8c JDBC Sink. */
final class GBase8cSinkSupport {

    private static final String DEFAULT_SCHEMA = "public";

    private GBase8cSinkSupport() {
    }

    static boolean accepts(JdbcConnectionConfig config) {
        if (config == null) {
            return false;
        }
        if (hasText(config.getDialect())) {
            return DatabaseIdentifier.GBASE8C.equalsIgnoreCase(config.getDialect());
        }
        return GBase8cJdbcUrl.accepts(config.getUrl());
    }

    /**
     * An implicit target keeps only the source table name. Source database/schema metadata must
     * never choose the GBase 8c target schema merely because two databases happen to share a name.
     */
    static TablePath resolveImplicitTargetPath(
            JdbcConnectionConfig config,
            TablePath sourcePath) {
        if (config == null || sourcePath == null) {
            return sourcePath;
        }
        return TablePath.of(
                requireUrlDatabase(config),
                defaultSchema(config),
                requireTableName(sourcePath));
    }

    /** Explicit schema.table mappings are preserved, but one connection cannot change database. */
    static TablePath resolveExplicitTargetPath(
            JdbcConnectionConfig config,
            TablePath targetPath) {
        if (config == null || targetPath == null) {
            return targetPath;
        }
        String database = requireUrlDatabase(config);
        if (hasText(targetPath.getDatabaseName())
                && !database.equals(targetPath.getDatabaseName().trim())) {
            throw crossDatabase(database, targetPath.getDatabaseName());
        }
        String schema = hasText(targetPath.getSchemaName())
                ? targetPath.getSchemaName().trim()
                : defaultSchema(config);
        return TablePath.of(database, schema, requireTableName(targetPath));
    }

    /** Normalizes an already prepared target while preserving its selected target schema. */
    static TablePath resolveTargetPath(
            JdbcConnectionConfig config,
            TablePath tablePath) {
        if (config == null || tablePath == null) {
            return tablePath;
        }
        String database = requireUrlDatabase(config);
        if (hasText(tablePath.getDatabaseName())
                && !database.equals(tablePath.getDatabaseName().trim())) {
            throw crossDatabase(database, tablePath.getDatabaseName());
        }
        String schema = hasText(tablePath.getSchemaName())
                ? tablePath.getSchemaName().trim()
                : defaultSchema(config);
        return TablePath.of(database, schema, requireTableName(tablePath));
    }

    /** Builds the same compatibility-aware CREATE TABLE statement used by GBase8cCatalog. */
    static String resolveCreateTableSql(
            JdbcConnectionConfig config,
            CatalogTable table,
            GBase8cCompatibilityMode compatibilityMode) {
        if (config == null || table == null || compatibilityMode == null) {
            return null;
        }
        TablePath targetPath = resolveTargetPath(config, table.getTablePath());
        CatalogTable ddlTable = table.getTablePath().equals(targetPath)
                ? table
                : table.withPath(targetPath);
        return new GBase8cCreateTableSqlBuilder(
                targetPath,
                ddlTable,
                compatibilityMode)
                .build();
    }

    private static String defaultSchema(JdbcConnectionConfig config) {
        return hasText(config.getSchema()) ? config.getSchema().trim() : DEFAULT_SCHEMA;
    }

    private static String requireUrlDatabase(JdbcConnectionConfig config) {
        String database = GBase8cJdbcUrl.databaseName(config.getUrl());
        if (!hasText(database)) {
            throw new IllegalArgumentException("GBase 8c Sink JDBC URL must specify database");
        }
        return database.trim();
    }

    private static String requireTableName(TablePath tablePath) {
        if (!hasText(tablePath.getTableName())) {
            throw new IllegalArgumentException("GBase 8c target table name must not be empty");
        }
        return tablePath.getTableName().trim();
    }

    private static IllegalArgumentException crossDatabase(
            String urlDatabase,
            String targetDatabase) {
        return new IllegalArgumentException(
                "GBase 8c explicit target database must match Sink JDBC URL database; "
                        + "urlDatabase="
                        + urlDatabase
                        + ", targetDatabase="
                        + targetDatabase);
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
