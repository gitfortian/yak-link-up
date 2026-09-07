package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8s.GBase8sJdbcUrl;

/** Target-path normalization and validation for the GBase 8s existing-table JDBC Sink. */
final class GBase8sSinkSupport {

    private GBase8sSinkSupport() {
    }

    static boolean accepts(JdbcConnectionConfig config) {
        if (config == null) {
            return false;
        }
        if (hasText(config.getDialect())) {
            return DatabaseIdentifier.GBASE8S.equalsIgnoreCase(config.getDialect());
        }
        return GBase8sJdbcUrl.accepts(config.getUrl());
    }

    /**
     * An implicit target keeps only the source table name. Source database/schema/owner metadata
     * must never leak into a different GBase 8s target connection.
     */
    static TablePath resolveImplicitTargetPath(
            JdbcConnectionConfig config,
            TablePath sourcePath) {
        if (config == null || sourcePath == null) {
            return sourcePath;
        }
        return TablePath.of(
                requireUrlDatabase(config),
                defaultOwner(config),
                requireTableName(sourcePath));
    }

    /**
     * Explicit target mappings may choose an owner, but the database must remain the database from
     * the Sink JDBC URL.
     */
    static TablePath resolveExplicitTargetPath(
            JdbcConnectionConfig config,
            TablePath targetPath) {
        if (config == null || targetPath == null) {
            return targetPath;
        }
        validateExplicitTargetPath(config, targetPath);
        String owner = hasText(targetPath.getSchemaName())
                ? targetPath.getSchemaName().trim()
                : defaultOwner(config);
        return TablePath.of(
                requireUrlDatabase(config),
                owner,
                requireTableName(targetPath));
    }

    /** Normalizes an already prepared target while preserving its selected owner. */
    static TablePath resolvePreparedTargetPath(
            JdbcConnectionConfig config,
            TablePath tablePath) {
        if (config == null || tablePath == null) {
            return tablePath;
        }
        String database = requireUrlDatabase(config);
        if (hasText(tablePath.getDatabaseName())
                && !database.equalsIgnoreCase(tablePath.getDatabaseName().trim())) {
            throw crossDatabase(database, tablePath.getDatabaseName());
        }
        String owner = hasText(tablePath.getSchemaName())
                ? tablePath.getSchemaName().trim()
                : defaultOwner(config);
        return TablePath.of(database, owner, requireTableName(tablePath));
    }

    static void validateExplicitTargetPath(
            JdbcConnectionConfig config,
            TablePath tablePath) {
        if (config == null || tablePath == null) {
            return;
        }
        String database = requireUrlDatabase(config);
        if (hasText(tablePath.getDatabaseName())
                && !database.equalsIgnoreCase(tablePath.getDatabaseName().trim())) {
            throw crossDatabase(database, tablePath.getDatabaseName());
        }
    }

    private static String defaultOwner(JdbcConnectionConfig config) {
        if (hasText(config.getSchema())) {
            return config.getSchema().trim();
        }
        return hasText(config.getUsername())
                ? config.getUsername().trim()
                : null;
    }

    private static String requireUrlDatabase(JdbcConnectionConfig config) {
        String database = GBase8sJdbcUrl.databaseName(config.getUrl());
        if (!hasText(database)) {
            throw new IllegalArgumentException("GBase 8s Sink JDBC URL must specify database");
        }
        return database.trim();
    }

    private static String requireTableName(TablePath tablePath) {
        if (!hasText(tablePath.getTableName())) {
            throw new IllegalArgumentException("GBase 8s target table name must not be empty");
        }
        return tablePath.getTableName().trim();
    }

    private static IllegalArgumentException crossDatabase(
            String urlDatabase,
            String targetDatabase) {
        return new IllegalArgumentException(
                "GBase 8s explicit target database must match Sink JDBC URL database; "
                        + "urlDatabase="
                        + urlDatabase
                        + ", targetDatabase="
                        + targetDatabase);
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
