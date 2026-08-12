package com.link.up.connector.doris.catalog;

import com.link.up.api.table.catalog.*;
import com.link.up.api.table.type.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Doris CREATE TABLE SQL 构造器。
 *
 * <p>生成 Doris OLAP 引擎建表语句，包含：
 *
 * <ol>
 *   <li>字段定义（类型、非空、默认值）；
 *   <li>主键（UNIQUE KEY）或首字段（DUPLICATE KEY）；
 *   <li>DISTRIBUTED BY HASH；
 *   <li>PROPERTIES（replication_allocation 等）；
 *   <li>表注释。
 * </ol>
 */
public final class DorisCreateTableSqlBuilder {

    /**
     * 表选项：Doris 表模型类型。
     */
    public static final String TABLE_OPTION_KEY_TYPE =
            "doris.key-type";

    /**
     * 表选项：副本分配。
     */
    public static final String TABLE_OPTION_REPLICATION =
            "doris.replication-allocation";

    private static final Pattern NUMBER_PATTERN =
            Pattern.compile(
                    "[-+]?\\d+(\\.\\d+)?");

    private final TablePath tablePath;
    private final CatalogTable catalogTable;
    private final DorisTypeMapper typeMapper;

    public DorisCreateTableSqlBuilder(
            TablePath tablePath,
            CatalogTable catalogTable,
            DorisTypeMapper typeMapper) {

        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
        this.typeMapper = typeMapper;
    }

    private static String quoteIdentifier(String value) {
        return "`"
                + value.replace("`", "``")
                + "`";
    }

    private static String quoteTable(TablePath tablePath) {
        return quoteIdentifier(
                tablePath.getDatabaseName())
                + "."
                + quoteIdentifier(
                tablePath.getTableName());
    }

    private static String escapeString(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("'", "''");
    }

    private static boolean hasText(String value) {
        return value != null
                && !value.trim().isEmpty();
    }

    private static String getOrDefault(
            Map<String, String> options,
            String key,
            String defaultValue) {

        String value = options.get(key);

        return hasText(value) ? value : defaultValue;
    }

    private static boolean isNumeric(SqlType sqlType) {
        return sqlType == SqlType.TINYINT
                || sqlType == SqlType.SMALLINT
                || sqlType == SqlType.INT
                || sqlType == SqlType.BIGINT
                || sqlType == SqlType.FLOAT
                || sqlType == SqlType.DOUBLE
                || sqlType == SqlType.DECIMAL;
    }

    public String build() {
        TableSchema schema =
                catalogTable.getTableSchema();

        List<String> definitions =
                new ArrayList<>();

        boolean preserveSourceType =
                "doris".equalsIgnoreCase(
                        catalogTable.getOptions()
                                .get("dialect"));

        for (Column column : schema.getColumns()) {
            definitions.add(
                    buildColumn(column, preserveSourceType));
        }

        StringBuilder sql = new StringBuilder();

        sql.append("CREATE TABLE ")
                .append(quoteTable(tablePath))
                .append(" (\n    ")
                .append(
                        String.join(
                                ",\n    ",
                                definitions))
                .append("\n)");

        /*
         * 确定 Key 类型和 Key 列。
         *
         * 优先使用主键作为 UNIQUE KEY；
         * 没有主键时使用首字段作为 DUPLICATE KEY。
         */
        PrimaryKey primaryKey =
                schema.getPrimaryKey();

        String keyType;
        String keyColumns;

        if (primaryKey != null
                && !primaryKey.getColumnNames().isEmpty()) {

            keyType = "UNIQUE KEY";
            keyColumns = primaryKey.getColumnNames()
                    .stream()
                    .map(
                            DorisCreateTableSqlBuilder
                                    ::quoteIdentifier)
                    .collect(
                            Collectors.joining(", "));

        } else {
            keyType = "DUPLICATE KEY";
            keyColumns = quoteIdentifier(
                    schema.getColumn(0).getName());
        }

        sql.append(" ENGINE=OLAP ")
                .append(keyType)
                .append("(")
                .append(keyColumns)
                .append(")");

        /*
         * DISTRIBUTED BY HASH
         *
         * 使用 Key 列作为分布键。
         */
        sql.append(" DISTRIBUTED BY HASH(")
                .append(keyColumns)
                .append(")");

        /*
         * PROPERTIES
         */
        String replication =
                getOrDefault(
                        catalogTable.getOptions(),
                        TABLE_OPTION_REPLICATION,
                        "tag.location.default: 1");

        sql.append(" PROPERTIES (\"replication_allocation\" = \"")
                .append(replication)
                .append("\")");

        /*
         * 表注释
         */
        if (hasText(catalogTable.getComment())) {
            sql.append(" COMMENT '")
                    .append(
                            escapeString(
                                    catalogTable.getComment()))
                    .append('\'');
        }

        return sql.append(';')
                .toString();
    }

    /**
     * 构建可复用的字段定义，用于 ALTER TABLE ADD COLUMN。
     */
    public String buildColumnDefinition(Column column) {
        boolean preserveSourceType =
                "doris".equalsIgnoreCase(
                        catalogTable.getOptions()
                                .get("dialect"));

        return buildColumn(column, preserveSourceType);
    }

    private String buildColumn(
            Column column,
            boolean preserveSourceType) {

        List<String> parts = new ArrayList<>();

        parts.add(
                quoteIdentifier(
                        column.getName()));

        parts.add(
                typeMapper.toDorisType(
                        column,
                        preserveSourceType));

        parts.add(
                column.isNullable()
                        ? "NULL"
                        : "NOT NULL");

        if (!column.isAutoIncrement()
                && column.getDefaultValue() != null) {

            parts.add(
                    "DEFAULT "
                            + formatDefaultValue(column));
        }

        if (hasText(column.getComment())) {
            parts.add(
                    "COMMENT '"
                            + escapeString(
                            column.getComment())
                            + "'");
        }

        return String.join(" ", parts);
    }

    private String formatDefaultValue(Column column) {
        Object value = column.getDefaultValue();

        if (value instanceof Number) {
            return value.toString();
        }

        if (value instanceof Boolean) {
            return (Boolean) value ? "1" : "0";
        }

        String text =
                String.valueOf(value).trim();

        String upper =
                text.toUpperCase(Locale.ROOT);

        if ("NULL".equals(upper)
                || "CURRENT_TIMESTAMP".equals(upper)
                || upper.startsWith("CURRENT_TIMESTAMP(")
                || "CURRENT_DATE".equals(upper)
                || "CURRENT_TIME".equals(upper)) {

            return text;
        }

        SqlType sqlType =
                column.getDataType()
                        .getSqlType();

        if (isNumeric(sqlType)
                && NUMBER_PATTERN
                .matcher(text)
                .matches()) {

            return text;
        }

        return "'"
                + escapeString(text)
                + "'";
    }
}
