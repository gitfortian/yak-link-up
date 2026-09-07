package com.link.up.connector.jdbc.catalog.gbase.gbase8s;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8s.GBase8sTypeMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Builds the conservative CREATE TABLE statement used by GBase 8s automatic target creation.
 *
 * <p>The builder copies only column names, portable target types, nullability and an optional
 * primary key. Source defaults, SERIAL/BIGSERIAL generation, comments, indexes, partitions and
 * compatibility-mode DDL are intentionally not copied.</p>
 */
public final class GBase8sCreateTableSqlBuilder {

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final GBase8sTypeMapper typeMapper;
    private final boolean delimitedIdentifiers;

    public GBase8sCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            GBase8sTypeMapper typeMapper,
            boolean delimitedIdentifiers) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        if (catalogTable == null) {
            throw new IllegalArgumentException("catalogTable must not be null");
        }
        if (typeMapper == null) {
            throw new IllegalArgumentException("typeMapper must not be null");
        }
        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
        this.typeMapper = typeMapper;
        this.delimitedIdentifiers = delimitedIdentifiers;
    }

    public String build() {
        TableSchema schema = catalogTable.getTableSchema();
        if (schema == null || schema.getColumns().isEmpty()) {
            throw new IllegalArgumentException("GBase 8s CREATE TABLE requires at least one column");
        }

        List<String> definitions = new ArrayList<String>();
        for (Column column : schema.getColumns()) {
            definitions.add(buildColumnDefinition(column));
        }

        PrimaryKey primaryKey = schema.getPrimaryKey();
        if (primaryKey != null) {
            definitions.add(buildPrimaryKey(primaryKey, schema));
        }

        return "CREATE TABLE "
                + quoteTable(tablePath)
                + " (\n    "
                + String.join(",\n    ", definitions)
                + "\n);";
    }

    public String buildColumnDefinition(Column column) {
        if (column == null) {
            throw new IllegalArgumentException("column must not be null");
        }
        List<String> parts = new ArrayList<String>();
        parts.add(quoteIdentifier(column.getName()));
        parts.add(typeMapper.toDatabaseType(column));
        parts.add(column.isNullable() ? "NULL" : "NOT NULL");
        return String.join(" ", parts);
    }

    private String buildPrimaryKey(
            PrimaryKey primaryKey,
            TableSchema schema) {
        for (String columnName : primaryKey.getColumnNames()) {
            Column column = findColumn(schema, columnName);
            if (column == null) {
                throw new IllegalArgumentException(
                        "GBase 8s primary key column is missing from table schema: " + columnName);
            }
            String targetType = typeMapper.toDatabaseType(column);
            if ("TEXT".equalsIgnoreCase(targetType)
                    || "BYTE".equalsIgnoreCase(targetType)) {
                throw new UnsupportedOperationException(
                        "GBase 8s automatic target primary key cannot use "
                                + targetType
                                + " column '"
                                + columnName
                                + "'; configure a bounded scalar target type or set create_primary_key=false");
            }
        }

        String columns = primaryKey.getColumnNames().stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        return "PRIMARY KEY (" + columns + ")";
    }

    private static Column findColumn(
            TableSchema schema,
            String columnName) {
        for (Column column : schema.getColumns()) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }

    private String quoteTable(TablePath path) {
        if (!hasText(path.getTableName())) {
            throw new IllegalArgumentException("GBase 8s target table name must not be empty");
        }
        String owner = path.getSchemaName();
        return hasText(owner)
                ? quoteIdentifier(owner) + "." + quoteIdentifier(path.getTableName())
                : quoteIdentifier(path.getTableName());
    }

    private String quoteIdentifier(String identifier) {
        if (!hasText(identifier)) {
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

    private static boolean isSimpleIdentifier(String value) {
        if (!hasText(value)) {
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

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
