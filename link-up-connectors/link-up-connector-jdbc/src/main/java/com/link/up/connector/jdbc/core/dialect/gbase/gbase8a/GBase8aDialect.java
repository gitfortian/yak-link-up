package com.link.up.connector.jdbc.core.dialect.gbase.gbase8a;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.gbase.gbase8a.GBase8aCatalog;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.converter.JdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * GBase 8a bounded/offline JDBC dialect.
 *
 * <p>The adapter supports bounded reads and an existing-table INSERT/batch Sink. MPP-native loading,
 * structure-changing Sink DDL, UPSERT/MERGE, CDC and streaming job semantics remain separate stages.</p>
 */
public final class GBase8aDialect implements JdbcDialect {

    private final String defaultDatabase;
    private final GBase8aTypeMapper typeMapper;

    public GBase8aDialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        if (!GBase8aJdbcUrl.accepts(connectionConfig.getUrl())) {
            throw new IllegalArgumentException(
                    "非法 GBase 8a JDBC URL：" + connectionConfig.getUrl());
        }

        String database = GBase8aJdbcUrl.databaseName(connectionConfig.getUrl());
        if (!JdbcDialect.hasText(database)) {
            throw new IllegalArgumentException(
                    "GBase 8a JDBC URL 必须指定 database");
        }

        this.defaultDatabase = database.trim();
        this.typeMapper = new GBase8aTypeMapper();
    }

    @Override
    public String name() {
        return DatabaseIdentifier.GBASE8A;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {
        String database = GBase8aJdbcUrl.databaseName(connectionConfig.getUrl());

        return new GBase8aCatalog(
                catalogName,
                new JdbcCatalogConfig(
                        connectionConfig.getUrl(),
                        connectionConfig.getUsername(),
                        connectionConfig.getPassword(),
                        connectionConfig.getDriverName(),
                        resolveConnectionProperties(connectionConfig.getProperties()),
                        false),
                database);
    }

    @Override
    public JdbcTypeMapper typeMapper() {
        return typeMapper;
    }

    @Override
    public JdbcRowConverter rowConverter() {
        return new GBase8aJdbcRowConverter();
    }

    /** GBase 8a uses database.table and does not expose a separate schema layer. */
    @Override
    public TablePath parseTablePath(String tablePath) {
        if (!JdbcDialect.hasText(tablePath)) {
            throw new IllegalArgumentException("tablePath must not be empty");
        }
        String[] parts = tablePath.trim().split("\\.", -1);
        switch (parts.length) {
            case 1:
                return TablePath.of(normalizePart(parts[0]));
            case 2:
                return TablePath.of(
                        normalizePart(parts[0]),
                        normalizePart(parts[1]));
            default:
                throw new IllegalArgumentException(
                        "非法 GBase 8a 表路径，仅支持 table 或 database.table：" + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (!JdbcDialect.hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "`" + identifier.trim().replace("`", "``") + "`";
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        if (!JdbcDialect.hasText(tablePath.getTableName())) {
            throw new IllegalArgumentException("table name must not be empty");
        }

        String database = tablePath.getDatabaseName();
        if (!JdbcDialect.hasText(database)) {
            database = tablePath.getSchemaName();
        }
        if (!JdbcDialect.hasText(database)) {
            database = defaultDatabase;
        }

        return quoteIdentifier(database)
                + "."
                + quoteIdentifier(tablePath.getTableName());
    }

    /**
     * GBase JDBC streaming reads require Integer.MIN_VALUE rather than an ordinary positive fetch
     * size. A positive Link-Up fetch_size therefore opts into the vendor's streaming result mode.
     */
    @Override
    public PreparedStatement prepareReadStatement(
            Connection connection,
            String sql,
            int fetchSize) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(Integer.MIN_VALUE);
        }
        return statement;
    }

    @Override
    public Map<String, String> defaultConnectionProperties() {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("tinyInt1isBit", "false");
        properties.put("yearIsDateType", "false");
        properties.put("rewriteBatchedStatements", "true");
        return Collections.unmodifiableMap(properties);
    }

    private static String normalizePart(String part) {
        String value = part == null ? "" : part.trim();
        if (value.length() >= 2
                && value.charAt(0) == '`'
                && value.charAt(value.length() - 1) == '`') {
            value = value.substring(1, value.length() - 1)
                    .replace("``", "`");
        }
        if (!JdbcDialect.hasText(value)) {
            throw new IllegalArgumentException("GBase 8a table path contains an empty identifier");
        }
        return value;
    }
}
