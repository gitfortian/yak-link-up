package com.link.up.connector.clickhouse.config;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable runtime configuration for the bounded ClickHouse Source. */
public final class ClickHouseSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> hosts;
    private final String username;
    private final String password;
    private final String serverTimeZone;
    private final Map<String, String> clientConfig;
    private final List<ClickHouseSourceTableConfig> tableConfigs;
    private final Map<TablePath, ClickHouseSourceTableConfig> tableConfigIndex;

    private ClickHouseSourceConfig(
            List<String> hosts,
            String username,
            String password,
            String serverTimeZone,
            Map<String, String> clientConfig,
            List<ClickHouseSourceTableConfig> tableConfigs) {
        this.hosts = Collections.unmodifiableList(new ArrayList<String>(hosts));
        this.username = username;
        this.password = password;
        this.serverTimeZone = serverTimeZone;
        this.clientConfig =
                Collections.unmodifiableMap(new LinkedHashMap<String, String>(clientConfig));
        this.tableConfigs =
                Collections.unmodifiableList(
                        new ArrayList<ClickHouseSourceTableConfig>(tableConfigs));
        Map<TablePath, ClickHouseSourceTableConfig> index =
                new LinkedHashMap<TablePath, ClickHouseSourceTableConfig>();
        for (ClickHouseSourceTableConfig tableConfig : tableConfigs) {
            if (index.put(tableConfig.getTablePath(), tableConfig) != null) {
                throw new IllegalArgumentException(
                        "Duplicate ClickHouse source dataset path: " + tableConfig.getTablePath());
            }
        }
        this.tableConfigIndex = Collections.unmodifiableMap(index);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static ClickHouseSourceConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");

        List<String> hosts = parseHosts(requireText(config.get(ClickHouseSourceOptions.HOST), "host"));
        String username = requireText(config.get(ClickHouseSourceOptions.USERNAME), "username");
        String password = config.get(ClickHouseSourceOptions.PASSWORD);
        String serverTimeZone =
                normalize(config.getOptional(ClickHouseSourceOptions.SERVER_TIME_ZONE).orElse(null));
        Map<String, String> clientConfig =
                config.getOptional(ClickHouseSourceOptions.CLICKHOUSE_CONFIG)
                        .orElse(Collections.<String, String>emptyMap());

        String commonTablePath =
                normalize(config.getOptional(ClickHouseSourceOptions.TABLE_PATH).orElse(null));
        String commonSql = normalize(config.getOptional(ClickHouseSourceOptions.SQL).orElse(null));
        String commonFilter = normalize(config.get(ClickHouseSourceOptions.FILTER_QUERY));
        List<String> commonPartitions =
                config.getOptional(ClickHouseSourceOptions.PARTITION_LIST)
                        .orElse(Collections.<String>emptyList());
        int commonSplitSize = config.get(ClickHouseSourceOptions.SPLIT_SIZE);
        int commonBatchSize = config.get(ClickHouseSourceOptions.BATCH_SIZE);
        validatePositive(commonSplitSize, "split.size");
        validatePositive(commonBatchSize, "batch_size");

        List<Map> tableList =
                config.getOptional(ClickHouseSourceOptions.TABLE_LIST)
                        .orElse(Collections.<Map>emptyList());
        if (!tableList.isEmpty() && (commonTablePath != null || commonSql != null)) {
            throw new IllegalArgumentException(
                    "ClickHouse source table_list cannot be combined with top-level table_path or sql");
        }

        List<ClickHouseSourceTableConfig> tables =
                new ArrayList<ClickHouseSourceTableConfig>();
        if (tableList.isEmpty()) {
            if (commonTablePath == null && commonSql == null) {
                throw new IllegalArgumentException(
                        "ClickHouse source requires table_path, sql, or table_list");
            }
            tables.add(
                    buildTableConfig(
                            0,
                            commonTablePath,
                            commonSql,
                            commonFilter,
                            commonPartitions,
                            commonSplitSize,
                            commonBatchSize));
        } else {
            int index = 0;
            for (Map item : tableList) {
                if (item == null) {
                    throw new IllegalArgumentException("table_list item must not be null");
                }
                String tablePath = normalize(stringValue(item.get("table_path")));
                String sql = normalize(stringValue(item.get("sql")));
                if (tablePath == null && sql == null) {
                    throw new IllegalArgumentException(
                            "table_list[] requires table_path or sql");
                }
                String filter = normalize(stringValue(item.get("filter_query")));
                if (filter == null) {
                    filter = commonFilter;
                }
                List<String> partitions = listOfStrings(item.get("partition_list"));
                if (partitions.isEmpty()) {
                    partitions = commonPartitions;
                }
                int splitSize =
                        intValue(
                                firstNonNull(item.get("split_size"), item.get("split.size")),
                                commonSplitSize,
                                "table_list[].split_size");
                int batchSize =
                        intValue(item.get("batch_size"), commonBatchSize, "table_list[].batch_size");
                tables.add(
                        buildTableConfig(
                                index++,
                                tablePath,
                                sql,
                                filter,
                                partitions,
                                splitSize,
                                batchSize));
            }
        }

        return new ClickHouseSourceConfig(
                hosts,
                username,
                password == null ? "" : password,
                serverTimeZone,
                clientConfig,
                tables);
    }

    private static ClickHouseSourceTableConfig buildTableConfig(
            int index,
            String tablePathText,
            String sql,
            String filter,
            List<String> partitions,
            int splitSize,
            int batchSize) {
        validatePositive(splitSize, "split size");
        validatePositive(batchSize, "batch size");

        boolean synthetic = tablePathText == null;
        TablePath tablePath =
                synthetic
                        ? TablePath.of("__query__", "clickhouse_query_" + index)
                        : parseTablePath(tablePathText);

        if (!synthetic && tablePath.getSchemaName() != null) {
            throw new IllegalArgumentException(
                    "ClickHouse table_path must use database.table form: " + tablePathText);
        }
        if (sql == null && tablePath.getDatabaseName() == null) {
            throw new IllegalArgumentException(
                    "ClickHouse table mode requires database.table: " + tablePathText);
        }

        return new ClickHouseSourceTableConfig(
                tablePath,
                synthetic,
                sql,
                filter,
                normalizePartitions(partitions),
                splitSize,
                batchSize);
    }

    private static TablePath parseTablePath(String value) {
        try {
            return TablePath.parse(value);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "Invalid ClickHouse table_path, expected database.table: " + value,
                    failure);
        }
    }

    private static List<String> parseHosts(String value) {
        List<String> result = new ArrayList<String>();
        for (String item : value.split(",")) {
            String host = normalize(item);
            if (host == null) {
                continue;
            }
            while (host.endsWith("/")) {
                host = host.substring(0, host.length() - 1);
            }
            result.add(host);
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("host must contain at least one ClickHouse node");
        }
        return result;
    }

    private static List<String> normalizePartitions(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<String>();
        for (String value : values) {
            String normalized = normalize(value);
            if (normalized == null) {
                throw new IllegalArgumentException("partition_list values must not be blank");
            }
            result.add(normalized);
        }
        return result;
    }

    private static List<String> listOfStrings(Object value) {
        if (value == null) {
            return Collections.emptyList();
        }
        if (!(value instanceof List)) {
            throw new IllegalArgumentException("partition_list must be a list");
        }
        List<?> source = (List<?>) value;
        List<String> result = new ArrayList<String>(source.size());
        for (Object item : source) {
            result.add(item == null ? null : String.valueOf(item));
        }
        return result;
    }

    private static Object firstNonNull(Object first, Object second) {
        return first != null ? first : second;
    }

    private static int intValue(Object value, int defaultValue, String name) {
        if (value == null) {
            return defaultValue;
        }
        final int result;
        if (value instanceof Number) {
            result = ((Number) value).intValue();
        } else {
            try {
                result = Integer.parseInt(String.valueOf(value));
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException(name + " must be an integer", failure);
            }
        }
        validatePositive(result, name);
        return result;
    }

    private static void validatePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be greater than 0");
        }
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static String requireText(String value, String name) {
        String normalized = normalize(value);
        if (normalized == null) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return normalized;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getServerTimeZone() {
        return serverTimeZone;
    }

    public Map<String, String> getClientConfig() {
        return clientConfig;
    }

    public List<ClickHouseSourceTableConfig> getTableConfigs() {
        return tableConfigs;
    }

    public ClickHouseSourceTableConfig getTableConfig(TablePath tablePath) {
        ClickHouseSourceTableConfig result = tableConfigIndex.get(tablePath);
        if (result == null) {
            throw new IllegalArgumentException(
                    "Unknown ClickHouse source dataset: " + tablePath);
        }
        return result;
    }
}
