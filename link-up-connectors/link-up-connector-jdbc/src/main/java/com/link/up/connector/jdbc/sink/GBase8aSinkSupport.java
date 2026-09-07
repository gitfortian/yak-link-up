package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8a.GBase8aJdbcUrl;

/** Target-path normalization and validation for the GBase 8a existing-table JDBC Sink. */
final class GBase8aSinkSupport {

    private GBase8aSinkSupport() {
    }

    static boolean accepts(JdbcConnectionConfig config) {
        if (config == null) {
            return false;
        }
        if (hasText(config.getDialect())) {
            return DatabaseIdentifier.GBASE8A.equalsIgnoreCase(config.getDialect());
        }
        return GBase8aJdbcUrl.accepts(config.getUrl());
    }

    /**
     * The Sink JDBC URL owns the target database. Source database/schema metadata is never reused
     * implicitly across databases.
     */
    static TablePath resolveTargetPath(
            JdbcConnectionConfig config,
            TablePath tablePath) {
        if (config == null || tablePath == null) {
            return tablePath;
        }

        String database = GBase8aJdbcUrl.databaseName(config.getUrl());
        if (!hasText(database)) {
            return null;
        }
        if (!hasText(tablePath.getTableName())) {
            throw new IllegalArgumentException("GBase 8a target table name must not be empty");
        }

        return TablePath.of(database, tablePath.getTableName());
    }

    /**
     * Explicit target mappings may be unqualified or repeat the URL database, but may not redirect
     * one Sink connection to another database silently.
     */
    static void validateExplicitTargetPath(
            JdbcConnectionConfig config,
            TablePath tablePath) {
        if (config == null || tablePath == null) {
            return;
        }

        String urlDatabase = GBase8aJdbcUrl.databaseName(config.getUrl());
        if (!hasText(urlDatabase)) {
            throw new IllegalArgumentException("GBase 8a Sink JDBC URL must specify database");
        }

        String pathDatabase = tablePath.getDatabaseName();
        if (!hasText(pathDatabase)) {
            pathDatabase = tablePath.getSchemaName();
        }
        if (hasText(pathDatabase)
                && !urlDatabase.equals(pathDatabase.trim())) {
            throw new IllegalArgumentException(
                    "GBase 8a explicit target database must match Sink JDBC URL database; "
                            + "urlDatabase="
                            + urlDatabase
                            + ", targetDatabase="
                            + pathDatabase);
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
