package com.link.up.connector.doris.config;

import com.link.up.api.table.catalog.TablePath;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Per-table configuration for Doris bounded native Source. */
public final class DorisSourceTableConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String database;
    private final String table;
    private final String filterQuery;
    private final List<String> readFields;
    private final int requestTabletSize;
    private final int batchSize;
    private final long execMemLimit;

    public DorisSourceTableConfig(
            String database,
            String table,
            String filterQuery,
            List<String> readFields,
            int requestTabletSize,
            int batchSize,
            long execMemLimit) {
        this.database = requireText(database, "database");
        this.table = requireText(table, "table");
        this.filterQuery = filterQuery == null ? "" : filterQuery.trim();
        this.readFields = Collections.unmodifiableList(normalizeFields(readFields));
        if (requestTabletSize <= 0) {
            throw new IllegalArgumentException("requestTabletSize must be greater than 0");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        if (execMemLimit <= 0L) {
            throw new IllegalArgumentException("execMemLimit must be greater than 0");
        }
        this.requestTabletSize = requestTabletSize;
        this.batchSize = batchSize;
        this.execMemLimit = execMemLimit;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getFilterQuery() {
        return filterQuery;
    }

    public List<String> getReadFields() {
        return readFields;
    }

    public int getRequestTabletSize() { return requestTabletSize; }
    public int getBatchSize() { return batchSize; }
    public long getExecMemLimit() { return execMemLimit; }

    public TablePath getTablePath() {
        return TablePath.of(database, table);
    }

    private static List<String> normalizeFields(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<String>();
        for (String value : values) {
            String field = requireText(value, "read field");
            if (result.contains(field)) {
                throw new IllegalArgumentException("Duplicate Doris read field: " + field);
            }
            result.add(field);
        }
        return result;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value.trim();
    }
}
