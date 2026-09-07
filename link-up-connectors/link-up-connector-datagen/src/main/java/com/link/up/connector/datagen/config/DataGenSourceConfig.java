package com.link.up.connector.datagen.config;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Immutable bounded DataGen Source configuration, fully validated at construction. */
public final class DataGenSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String DEFAULT_TABLE_NAME = "datagen_table";

    private final List<ColumnRule> columns;
    private final List<List<Object>> presetRows;
    private final long rowCount;
    private final Integer splitCount;
    private final long readIntervalMillis;
    private final Long seed;
    private final String tableName;

    private DataGenSourceConfig(
            List<ColumnRule> columns,
            List<List<Object>> presetRows,
            long rowCount,
            Integer splitCount,
            long readIntervalMillis,
            Long seed,
            String tableName) {

        this.columns = Collections.unmodifiableList(new ArrayList<ColumnRule>(columns));
        this.presetRows = presetRows == null
                ? null
                : Collections.unmodifiableList(new ArrayList<List<Object>>(presetRows));
        this.rowCount = rowCount;
        this.splitCount = splitCount;
        this.readIntervalMillis = readIntervalMillis;
        this.seed = seed;
        this.tableName = tableName;
    }

    public static DataGenSourceConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");

        List<Map> rawSchema = config.get(DataGenSourceOptions.SCHEMA);
        if (rawSchema == null || rawSchema.isEmpty()) {
            throw new IllegalArgumentException("schema must not be empty");
        }

        List<List> rawRows = config.get(DataGenSourceOptions.ROWS);
        boolean presetRows = rawRows != null;

        if (presetRows && rawRows.isEmpty()) {
            throw new IllegalArgumentException("rows must not be empty when configured");
        }
        if (presetRows && config.getOptional(DataGenSourceOptions.ROW_COUNT).isPresent()) {
            throw new IllegalArgumentException("rows and row_count are mutually exclusive");
        }

        List<List<Object>> presets = null;
        long rowCount;
        if (presetRows) {
            presets = normalizeRows(rawRows);
            rowCount = presets.size();
        } else {
            rowCount = config.get(DataGenSourceOptions.ROW_COUNT);
            if (rowCount <= 0) {
                throw new IllegalArgumentException("row_count must be greater than 0");
            }
        }

        List<ColumnRule> columns = ColumnRule.parseSchema(rawSchema, rowCount, presetRows);
        validateRowWidth(columns, presetRows ? presets : null);

        Integer splitCount = config.getOptional(DataGenSourceOptions.SPLIT_COUNT).orElse(null);
        if (splitCount != null) {
            if (splitCount <= 0) {
                throw new IllegalArgumentException("split_count must be greater than 0");
            }
            if (splitCount > rowCount) {
                throw new IllegalArgumentException(
                        "split_count=" + splitCount + " must not exceed the total row count " + rowCount);
            }
        }

        long readIntervalMillis = config.get(DataGenSourceOptions.READ_INTERVAL_MILLIS);
        if (readIntervalMillis < 0) {
            throw new IllegalArgumentException("read_interval_millis must not be negative");
        }

        Long seed = config.getOptional(DataGenSourceOptions.SEED).orElse(null);

        String tableName = config.get(DataGenSourceOptions.TABLE_NAME);
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("table_name must not be blank");
        }
        tableName = tableName.trim();

        return new DataGenSourceConfig(
                columns,
                presets,
                rowCount,
                splitCount,
                readIntervalMillis,
                seed,
                tableName);
    }

    public List<ColumnRule> getColumns() {
        return columns;
    }

    public boolean hasPresetRows() {
        return presetRows != null;
    }

    public List<List<Object>> getPresetRows() {
        return presetRows == null
                ? Collections.<List<Object>>emptyList()
                : presetRows;
    }

    public long getRowCount() {
        return rowCount;
    }

    public Integer getSplitCount() {
        return splitCount;
    }

    public long getReadIntervalMillis() {
        return readIntervalMillis;
    }

    public Long getSeed() {
        return seed;
    }

    public String getTableName() {
        return tableName;
    }

    public TablePath getTablePath() {
        return TablePath.of(tableName);
    }

    private static List<List<Object>> normalizeRows(List<List> rawRows) {
        List<List<Object>> rows = new ArrayList<List<Object>>(rawRows.size());
        Set<Integer> widths = new HashSet<Integer>();
        for (int i = 0; i < rawRows.size(); i++) {
            List<?> rawRow = rawRows.get(i);
            if (rawRow == null) {
                throw new IllegalArgumentException("rows entry #" + i + " must not be null");
            }
            widths.add(rawRow.size());
            rows.add(new ArrayList<Object>(rawRow));
        }
        if (widths.size() > 1) {
            throw new IllegalArgumentException("rows entries must all have the same width");
        }
        return rows;
    }

    private static void validateRowWidth(List<ColumnRule> columns, List<List<Object>> presets) {
        if (presets == null) {
            return;
        }
        for (int i = 0; i < presets.size(); i++) {
            if (presets.get(i).size() != columns.size()) {
                throw new IllegalArgumentException(
                        "rows entry #" + i + " has " + presets.get(i).size()
                                + " values but the schema declares " + columns.size() + " columns");
            }
        }
    }

    @Override
    public String toString() {
        return "DataGenSourceConfig{"
                + "table=" + tableName
                + ", columns=" + columns.size()
                + ", rows=" + rowCount
                + ", preset=" + hasPresetRows()
                + ", seed=" + seed
                + '}';
    }
}
