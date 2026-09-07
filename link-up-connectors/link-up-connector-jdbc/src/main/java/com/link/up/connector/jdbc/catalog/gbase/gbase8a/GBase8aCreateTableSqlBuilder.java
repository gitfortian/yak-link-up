package com.link.up.connector.jdbc.catalog.gbase.gbase8a;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8a.GBase8aTypeMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds the conservative CREATE TABLE statement used by the GBase 8a automatic-target stage.
 *
 * <p>The builder intentionally copies only the relational shape needed for offline synchronization:
 * columns, target types, nullability and (when requested by the shared sink lifecycle) the primary
 * key. Source defaults, auto-increment/identity behavior, comments, indexes, foreign keys,
 * partitions and MPP distribution policy are not copied.</p>
 *
 * <p>No DISTRIBUTED BY or REPLICATED clause is generated. The target database therefore keeps its
 * normal GBase 8a default table-distribution behavior instead of Link-Up guessing a business hash
 * key.</p>
 */
public final class GBase8aCreateTableSqlBuilder {

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final GBase8aTypeMapper typeMapper;

    public GBase8aCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            GBase8aTypeMapper typeMapper) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        if (catalogTable == null) {
            throw new IllegalArgumentException("catalogTable must not be null");
        }
        if (typeMapper == null) {
            throw new IllegalArgumentException("typeMapper must not be null");
        }
        if (!hasText(tablePath.getDatabaseName())) {
            throw new IllegalArgumentException("GBase 8a CREATE TABLE requires database: " + tablePath);
        }
        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
        this.typeMapper = typeMapper;
    }

    public String build() {
        TableSchema schema = catalogTable.getTableSchema();
        if (schema == null || schema.getColumns() == null || schema.getColumns().isEmpty()) {
            throw new IllegalArgumentException("GBase 8a CREATE TABLE requires at least one column");
        }

        List<String> definitions = new ArrayList<String>();
        for (Column column : schema.getColumns()) {
            definitions.add(buildColumnDefinition(column));
        }

        PrimaryKey primaryKey = schema.getPrimaryKey();
        if (primaryKey != null && !primaryKey.getColumnNames().isEmpty()) {
            definitions.add(buildPrimaryKey(primaryKey));
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

    private static String buildPrimaryKey(PrimaryKey primaryKey) {
        String columns = primaryKey.getColumnNames().stream()
                .map(GBase8aCreateTableSqlBuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));
        return "PRIMARY KEY (" + columns + ")";
    }

    private static String quoteTable(TablePath path) {
        return quoteIdentifier(path.getDatabaseName())
                + "."
                + quoteIdentifier(path.getTableName());
    }

    private static String quoteIdentifier(String value) {
        if (!hasText(value)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "`" + value.trim().replace("`", "``") + "`";
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
