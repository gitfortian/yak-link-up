package com.link.up.connector.clickhouse.config;

import com.link.up.api.table.catalog.TablePath;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Per-dataset bounded ClickHouse Source configuration. */
public final class ClickHouseSourceTableConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final boolean syntheticTablePath;
    private final String sql;
    private final String filterQuery;
    private final List<String> partitionList;
    private final int splitSize;
    private final int batchSize;

    public ClickHouseSourceTableConfig(
            TablePath tablePath,
            boolean syntheticTablePath,
            String sql,
            String filterQuery,
            List<String> partitionList,
            int splitSize,
            int batchSize) {
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath must not be null");
        this.syntheticTablePath = syntheticTablePath;
        this.sql = normalize(sql);
        this.filterQuery = normalize(filterQuery);
        this.partitionList =
                Collections.unmodifiableList(
                        partitionList == null
                                ? Collections.<String>emptyList()
                                : new ArrayList<String>(partitionList));
        if (splitSize <= 0) {
            throw new IllegalArgumentException("split size must be greater than 0");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batch size must be greater than 0");
        }
        if (isSqlMode() && !this.partitionList.isEmpty()) {
            throw new IllegalArgumentException(
                    "partition_list is only supported for ClickHouse table mode; encode partition predicates in sql mode explicitly");
        }
        this.splitSize = splitSize;
        this.batchSize = batchSize;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public boolean isSyntheticTablePath() {
        return syntheticTablePath;
    }

    public String getSql() {
        return sql;
    }

    public boolean isSqlMode() {
        return sql != null;
    }

    public String getFilterQuery() {
        return filterQuery;
    }

    public List<String> getPartitionList() {
        return partitionList;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getDatabase() {
        return tablePath.getDatabaseName();
    }

    public String getTable() {
        return tablePath.getTableName();
    }
}
