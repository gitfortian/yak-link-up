package com.link.up.connector.clickhouse.source;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.connector.clickhouse.client.ClickHouseJdbcClient;
import com.link.up.connector.clickhouse.config.ClickHouseSourceTableConfig;

import java.util.ArrayList;
import java.util.List;

/** Builds deterministic bounded SELECT statements for ClickHouse splits. */
public final class ClickHouseSourceSqlBuilder {

    private ClickHouseSourceSqlBuilder() {
    }

    public static String build(
            ClickHouseSourceSplit split,
            ClickHouseSourceTableConfig tableConfig,
            CatalogTable catalogTable) {
        if (split == null || tableConfig == null || catalogTable == null) {
            throw new IllegalArgumentException("split, tableConfig and catalogTable must not be null");
        }

        if (split.getMode() == ClickHouseSourceSplit.Mode.SQL_QUERY) {
            StringBuilder sql =
                    new StringBuilder()
                            .append("SELECT * FROM (")
                            .append(ClickHouseJdbcClient.stripTrailingSemicolon(split.getQuerySql()))
                            .append(") AS _link_up_source");
            appendFilter(sql, tableConfig.getFilterQuery(), false);
            return sql.toString();
        }

        List<String> fields = new ArrayList<String>();
        for (Column column : catalogTable.getTableSchema().getColumns()) {
            fields.add(quoteIdentifier(column.getName()));
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("ClickHouse source schema must contain at least one column");
        }

        StringBuilder sql =
                new StringBuilder()
                        .append("SELECT ")
                        .append(String.join(", ", fields))
                        .append(" FROM ")
                        .append(quoteIdentifier(tableConfig.getDatabase()))
                        .append('.')
                        .append(quoteIdentifier(tableConfig.getTable()));

        boolean hasWhere = false;
        if (split.getMode() == ClickHouseSourceSplit.Mode.PARTS) {
            sql.append(" WHERE _part IN (");
            List<String> parts = split.getPartNames();
            for (int i = 0; i < parts.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(quoteLiteral(parts.get(i)));
            }
            sql.append(')');
            hasWhere = true;
        }
        appendFilter(sql, tableConfig.getFilterQuery(), hasWhere);
        return sql.toString();
    }

    private static void appendFilter(StringBuilder sql, String filter, boolean hasWhere) {
        if (filter == null || filter.trim().isEmpty()) {
            return;
        }
        sql.append(hasWhere ? " AND (" : " WHERE (")
                .append(filter.trim())
                .append(')');
    }

    static String quoteIdentifier(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("ClickHouse identifier must not be empty");
        }
        return "`" + value.replace("`", "``") + "`";
    }

    static String quoteLiteral(String value) {
        if (value == null) {
            throw new IllegalArgumentException("ClickHouse string literal must not be null");
        }
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }
}
