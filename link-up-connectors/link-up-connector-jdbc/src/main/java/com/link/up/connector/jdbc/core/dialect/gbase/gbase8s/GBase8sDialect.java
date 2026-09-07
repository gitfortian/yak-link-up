package com.link.up.connector.jdbc.core.dialect.gbase.gbase8s;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.gbase.gbase8s.GBase8sCatalog;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.converter.JdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * GBase 8s bounded/offline JDBC Source dialect.
 *
 * <p>Stage 1 targets the native GBase 8s JDBC protocol and normal SQL mode. Sink, schema-changing
 * DDL, SQLMODE compatibility expansion, CDC and realtime semantics remain separate stages.</p>
 */
public final class GBase8sDialect implements JdbcDialect {

    private final String databaseName;
    private final String defaultOwner;
    private final boolean delimitedIdentifiers;
    private final GBase8sTypeMapper typeMapper;

    public GBase8sDialect(JdbcConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("connectionConfig must not be null");
        }
        if (!GBase8sJdbcUrl.accepts(connectionConfig.getUrl())) {
            throw new IllegalArgumentException(
                    "非法 GBase 8s JDBC URL：" + connectionConfig.getUrl());
        }

        String database = GBase8sJdbcUrl.databaseName(connectionConfig.getUrl());
        if (!JdbcDialect.hasText(database)) {
            throw new IllegalArgumentException("GBase 8s Stage 1 JDBC URL 必须指定 database");
        }
        String server = GBase8sJdbcUrl.serverName(
                connectionConfig.getUrl(),
                connectionConfig.getProperties());
        if (!JdbcDialect.hasText(server)) {
            throw new IllegalArgumentException(
                    "GBase 8s JDBC URL/properties 必须指定 GBASEDBTSERVER");
        }

        this.databaseName = database.trim();
        this.defaultOwner = JdbcDialect.hasText(connectionConfig.getSchema())
                ? connectionConfig.getSchema().trim()
                : JdbcDialect.hasText(connectionConfig.getUsername())
                ? connectionConfig.getUsername().trim()
                : null;
        this.delimitedIdentifiers = GBase8sJdbcUrl.delimitedIdentifiersEnabled(
                connectionConfig.getUrl(),
                connectionConfig.getProperties());
        this.typeMapper = new GBase8sTypeMapper();
    }

    @Override
    public String name() {
        return DatabaseIdentifier.GBASE8S;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {
        String owner = JdbcDialect.hasText(connectionConfig.getSchema())
                ? connectionConfig.getSchema().trim()
                : connectionConfig.getUsername();
        return new GBase8sCatalog(
                catalogName,
                new JdbcCatalogConfig(
                        connectionConfig.getUrl(),
                        connectionConfig.getUsername(),
                        connectionConfig.getPassword(),
                        connectionConfig.getDriverName(),
                        connectionConfig.getProperties(),
                        false),
                GBase8sJdbcUrl.databaseName(connectionConfig.getUrl()),
                owner);
    }

    @Override
    public JdbcTypeMapper typeMapper() {
        return typeMapper;
    }

    @Override
    public JdbcRowConverter rowConverter() {
        return new GBase8sJdbcRowConverter();
    }

    /** GBase 8s Stage 1 accepts table, owner.table, or currentDatabase.owner.table. */
    @Override
    public TablePath parseTablePath(String tablePath) {
        if (!JdbcDialect.hasText(tablePath)) {
            throw new IllegalArgumentException("tablePath must not be empty");
        }

        List<String> parts = splitPath(tablePath.trim());
        switch (parts.size()) {
            case 1:
                return TablePath.of(normalizePart(parts.get(0)));
            case 2:
                return TablePath.of(
                        null,
                        normalizePart(parts.get(0)),
                        normalizePart(parts.get(1)));
            case 3:
                String database = normalizePart(parts.get(0));
                requireCurrentDatabase(database);
                return TablePath.of(
                        databaseName,
                        normalizePart(parts.get(1)),
                        normalizePart(parts.get(2)));
            default:
                throw new IllegalArgumentException(
                        "非法 GBase 8s 表路径：" + tablePath);
        }
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (!JdbcDialect.hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        String value = identifier.trim();
        if (delimitedIdentifiers) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        if (!isSimpleIdentifier(value)) {
            throw new IllegalArgumentException(
                    "GBase 8s JDBC 默认 DELIMIDENT=n，不支持需要双引号的标识符："
                            + identifier
                            + "；如确需大小写敏感/特殊标识符，请在 JDBC URL/properties 设置 DELIMIDENT=y");
        }
        return value.toLowerCase(Locale.ROOT);
    }

    /** Database is selected by the JDBC connection; SQL uses owner.table inside that database. */
    @Override
    public String tableIdentifier(TablePath tablePath) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        if (JdbcDialect.hasText(tablePath.getDatabaseName())) {
            requireCurrentDatabase(tablePath.getDatabaseName());
        }
        if (!JdbcDialect.hasText(tablePath.getTableName())) {
            throw new IllegalArgumentException("table name must not be empty");
        }

        String owner = JdbcDialect.hasText(tablePath.getSchemaName())
                ? tablePath.getSchemaName().trim()
                : defaultOwner;
        if (JdbcDialect.hasText(owner)) {
            return quoteIdentifier(owner)
                    + "."
                    + quoteIdentifier(tablePath.getTableName());
        }
        return quoteIdentifier(tablePath.getTableName());
    }

    private List<String> splitPath(String path) {
        if (!delimitedIdentifiers) {
            if (path.indexOf('"') >= 0) {
                throw new IllegalArgumentException(
                        "GBase 8s JDBC 默认 DELIMIDENT=n，table_path 不允许双引号标识符：" + path);
            }
            String[] raw = path.split("\\.", -1);
            List<String> parts = new ArrayList<String>(raw.length);
            for (String part : raw) {
                parts.add(part);
            }
            return parts;
        }

        List<String> parts = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < path.length(); i++) {
            char value = path.charAt(i);
            if (value == '"') {
                current.append(value);
                if (quoted && i + 1 < path.length() && path.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    quoted = !quoted;
                }
                continue;
            }
            if (value == '.' && !quoted) {
                if (current.length() == 0) {
                    throw new IllegalArgumentException("非法 GBase 8s 表路径：" + path);
                }
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(value);
            }
        }
        if (quoted || current.length() == 0) {
            throw new IllegalArgumentException("非法 GBase 8s 表路径：" + path);
        }
        parts.add(current.toString());
        return parts;
    }

    private String normalizePart(String part) {
        String value = part == null ? "" : part.trim();
        if (delimitedIdentifiers
                && value.length() >= 2
                && value.charAt(0) == '"'
                && value.charAt(value.length() - 1) == '"') {
            String unquoted = value.substring(1, value.length() - 1)
                    .replace("\"\"", "\"");
            if (!JdbcDialect.hasText(unquoted)) {
                throw new IllegalArgumentException("GBase 8s table path contains an empty identifier");
            }
            return unquoted;
        }
        if (!JdbcDialect.hasText(value)) {
            throw new IllegalArgumentException("GBase 8s table path contains an empty identifier");
        }
        return value.toLowerCase(Locale.ROOT);
    }

    private void requireCurrentDatabase(String requestedDatabase) {
        if (!JdbcDialect.hasText(requestedDatabase)
                || !databaseName.equalsIgnoreCase(requestedDatabase.trim())) {
            throw new IllegalArgumentException(
                    "GBase 8s Stage 1 不支持跨 database table_path；当前 JDBC database="
                            + databaseName
                            + "，请求 database="
                            + requestedDatabase);
        }
    }

    private static boolean isSimpleIdentifier(String value) {
        if (!JdbcDialect.hasText(value)) {
            return false;
        }
        char first = value.charAt(0);
        if (!(Character.isLetter(first) || first == '_')) {
            return false;
        }
        for (int i = 1; i < value.length(); i++) {
            char current = value.charAt(i);
            if (!(Character.isLetterOrDigit(current)
                    || current == '_'
                    || current == '$'
                    || current == '#')) {
                return false;
            }
        }
        return true;
    }
}
