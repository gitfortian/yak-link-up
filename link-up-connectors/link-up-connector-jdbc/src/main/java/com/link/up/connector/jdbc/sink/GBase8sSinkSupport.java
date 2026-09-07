package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.gbase.gbase8s.GBase8sCreateTableSqlBuilder;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8s.GBase8sJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8s.GBase8sTypeMapper;

import java.util.Locale;

/** Target-path and automatic-DDL support for the GBase 8s JDBC Sink. */
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
                requireTableName(config, sourcePath));
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
                ? normalizeIdentifier(config, targetPath.getSchemaName())
                : defaultOwner(config);
        return TablePath.of(
                requireUrlDatabase(config),
                owner,
                requireTableName(config, targetPath));
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
                ? normalizeIdentifier(config, tablePath.getSchemaName())
                : defaultOwner(config);
        return TablePath.of(database, owner, requireTableName(config, tablePath));
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

    /** Builds the same safe CREATE TABLE SQL used by {@code GBase8sCatalog#createTable}. */
    static String resolveCreateTableSql(
            JdbcConnectionConfig config,
            CatalogTable table) {
        if (config == null || table == null) {
            return null;
        }

        TablePath targetPath = resolvePreparedTargetPath(config, table.getTablePath());
        CatalogTable ddlTable = table.getTablePath().equals(targetPath)
                ? table
                : table.withPath(targetPath);
        boolean delimitedIdentifiers = GBase8sJdbcUrl.delimitedIdentifiersEnabled(
                config.getUrl(),
                config.getProperties());
        return new GBase8sCreateTableSqlBuilder(
                targetPath,
                ddlTable,
                new GBase8sTypeMapper(),
                delimitedIdentifiers)
                .build();
    }

    private static String defaultOwner(JdbcConnectionConfig config) {
        if (hasText(config.getSchema())) {
            return normalizeIdentifier(config, config.getSchema());
        }
        return hasText(config.getUsername())
                ? normalizeIdentifier(config, config.getUsername())
                : null;
    }

    private static String requireUrlDatabase(JdbcConnectionConfig config) {
        String database = GBase8sJdbcUrl.databaseName(config.getUrl());
        if (!hasText(database)) {
            throw new IllegalArgumentException("GBase 8s Sink JDBC URL must specify database");
        }
        return database.trim();
    }

    private static String requireTableName(
            JdbcConnectionConfig config,
            TablePath tablePath) {
        if (!hasText(tablePath.getTableName())) {
            throw new IllegalArgumentException("GBase 8s target table name must not be empty");
        }
        return normalizeIdentifier(config, tablePath.getTableName());
    }

    private static String normalizeIdentifier(
            JdbcConnectionConfig config,
            String value) {
        String normalized = value == null ? null : value.trim();
        if (!hasText(normalized)) {
            return normalized;
        }
        if (GBase8sJdbcUrl.delimitedIdentifiersEnabled(
                config.getUrl(),
                config.getProperties())) {
            return normalized;
        }
        return normalized.toLowerCase(Locale.ROOT);
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
