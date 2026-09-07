package com.link.up.connector.jdbc.catalog.gbase.gbase8c;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cCompatibilityMode;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cTargetTypeMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds the conservative, compatibility-aware CREATE TABLE used by GBase 8c automatic targets.
 *
 * <p>The builder copies only the relational shape needed to receive synchronized rows: column
 * names, mode-aware physical target types, nullability and an optional primary key. Source
 * defaults, identity/serial behavior, comments, indexes, foreign keys, partitions and distributed
 * physical-design clauses are intentionally not copied.</p>
 *
 * <p>No DISTRIBUTE BY / REPLICATION clause is generated. Distribution-key selection is a business
 * and workload decision and remains outside safe cross-database automatic table creation.</p>
 */
public final class GBase8cCreateTableSqlBuilder {

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final GBase8cCompatibilityMode compatibilityMode;
    private final GBase8cTargetTypeMapper typeMapper;

    public GBase8cCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            GBase8cCompatibilityMode compatibilityMode) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        if (catalogTable == null) {
            throw new IllegalArgumentException("catalogTable must not be null");
        }
        if (compatibilityMode == null) {
            throw new IllegalArgumentException("compatibilityMode must not be null");
        }
        if (!hasText(tablePath.getSchemaName())) {
            throw new IllegalArgumentException(
                    "GBase 8c CREATE TABLE requires target schema: " + tablePath);
        }
        if (!hasText(tablePath.getTableName())) {
            throw new IllegalArgumentException("GBase 8c CREATE TABLE requires target table name");
        }
        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
        this.compatibilityMode = compatibilityMode;
        this.typeMapper = new GBase8cTargetTypeMapper(compatibilityMode);
    }

    public GBase8cCompatibilityMode compatibilityMode() {
        return compatibilityMode;
    }

    public String build() {
        TableSchema schema = catalogTable.getTableSchema();
        if (schema == null || schema.getColumns() == null || schema.getColumns().isEmpty()) {
            throw new IllegalArgumentException("GBase 8c CREATE TABLE requires at least one column");
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
                + quoteIdentifier(tablePath.getSchemaName())
                + "."
                + quoteIdentifier(tablePath.getTableName())
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
                .map(GBase8cCreateTableSqlBuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));
        return "PRIMARY KEY (" + columns + ")";
    }

    private static String quoteIdentifier(String identifier) {
        if (!hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "\"" + identifier.trim().replace("\"", "\"\"") + "\"";
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
