package com.link.up.connector.jdbc.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.mysql.MySqlCatalog;
import com.link.up.connector.jdbc.catalog.mysql.MySqlCreateTableSqlBuilder;
import com.link.up.connector.jdbc.catalog.mysql.MySqlTypeMapper;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

/**
 * TiDB-specific target normalization for the shared JDBC sink.
 */
final class TiDbSinkSupport {

    private TiDbSinkSupport() {
    }

    static boolean accepts(
            JdbcConnectionConfig connectionConfig) {

        return connectionConfig != null
                && DatabaseIdentifier.TIDB.equalsIgnoreCase(
                        connectionConfig.getDialect());
    }

    static TablePath resolveTargetPath(
            JdbcConnectionConfig connectionConfig,
            TablePath tablePath) {

        if (connectionConfig == null
                || tablePath == null) {
            return tablePath;
        }

        String database =
                databaseFromUrl(
                        connectionConfig.getUrl());

        if (!hasText(database)) {
            database = connectionConfig.getSchema();
        }

        if (!hasText(database)) {
            database = tablePath.getDatabaseName();
        }

        if (!hasText(database)) {
            database = tablePath.getSchemaName();
        }

        if (!hasText(database)) {
            return null;
        }

        return TablePath.of(
                database.trim(),
                tablePath.getTableName());
    }

    static String resolveCreateTableSql(
            JdbcConnectionConfig connectionConfig,
            CatalogTable table) {

        if (connectionConfig == null
                || table == null) {
            return null;
        }

        TablePath targetPath =
                resolveTargetPath(
                        connectionConfig,
                        table.getTablePath());

        if (targetPath == null) {
            return null;
        }

        CatalogTable ddlTable =
                table.getTablePath().equals(targetPath)
                        ? table
                        : table.withPath(targetPath);

        String sourceDialect =
                ddlTable.getOptions()
                        .get(MySqlCatalog.TABLE_OPTION_DIALECT);

        if (DatabaseIdentifier.TIDB
                .equalsIgnoreCase(sourceDialect)) {

            ddlTable = ddlTable.toBuilder()
                    .option(
                            MySqlCatalog.TABLE_OPTION_DIALECT,
                            DatabaseIdentifier.MYSQL)
                    .build();
        }

        return new MySqlCreateTableSqlBuilder(
                targetPath,
                ddlTable,
                new MySqlTypeMapper(false))
                .build();
    }

    private static String databaseFromUrl(
            String url) {

        if (!hasText(url)) {
            return null;
        }

        String normalized = url.trim();
        int protocolSeparator =
                normalized.indexOf("://");

        if (protocolSeparator < 0) {
            return null;
        }

        int databaseStart =
                normalized.indexOf(
                        '/',
                        protocolSeparator + 3);

        if (databaseStart < 0
                || databaseStart
                == normalized.length() - 1) {
            return null;
        }

        int databaseEnd = normalized.length();

        int queryStart =
                normalized.indexOf(
                        '?',
                        databaseStart + 1);

        if (queryStart >= 0) {
            databaseEnd = queryStart;
        }

        int fragmentStart =
                normalized.indexOf(
                        '#',
                        databaseStart + 1);

        if (fragmentStart >= 0
                && fragmentStart < databaseEnd) {
            databaseEnd = fragmentStart;
        }

        String database =
                normalized.substring(
                        databaseStart + 1,
                        databaseEnd)
                        .trim();

        return hasText(database)
                ? database
                : null;
    }

    private static boolean hasText(String value) {
        return value != null
                && !value.trim().isEmpty();
    }
}
