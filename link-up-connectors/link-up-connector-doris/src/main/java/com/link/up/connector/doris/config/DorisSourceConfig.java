package com.link.up.connector.doris.config;

import com.link.up.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable runtime configuration for the Doris bounded native Source. */
public final class DorisSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> feNodes;
    private final int queryPort;
    private final String username;
    private final String password;
    private final List<DorisSourceTableConfig> tableConfigs;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int queryTimeoutSec;
    private final int requestRetries;

    private DorisSourceConfig(
            List<String> feNodes,
            int queryPort,
            String username,
            String password,
            List<DorisSourceTableConfig> tableConfigs,
            int connectTimeoutMs,
            int readTimeoutMs,
            int queryTimeoutSec,
            int requestRetries) {
        this.feNodes = Collections.unmodifiableList(new ArrayList<String>(feNodes));
        this.queryPort = queryPort;
        this.username = username;
        this.password = password;
        this.tableConfigs =
                Collections.unmodifiableList(new ArrayList<DorisSourceTableConfig>(tableConfigs));
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.queryTimeoutSec = queryTimeoutSec;
        this.requestRetries = requestRetries;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static DorisSourceConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");

        List<String> feNodes = parseFeNodes(config.get(DorisSourceOptions.FENODES));
        int queryPort = config.get(DorisSourceOptions.QUERY_PORT);
        String username = requireText(config.get(DorisSourceOptions.USERNAME), "username");
        String password = config.getOptional(DorisSourceOptions.PASSWORD).orElse("");
        String commonDatabase = config.getOptional(DorisSourceOptions.DATABASE).orElse(null);
        String commonFilter = config.get(DorisSourceOptions.FILTER_QUERY);
        List<String> commonFields =
                parseFields(config.getOptional(DorisSourceOptions.READ_FIELDS).orElse(null));
        int commonTabletSize = config.get(DorisSourceOptions.REQUEST_TABLET_SIZE);
        int commonBatchSize = config.get(DorisSourceOptions.BATCH_SIZE);
        long commonMemLimit = config.get(DorisSourceOptions.EXEC_MEM_LIMIT);

        String singleTable = config.getOptional(DorisSourceOptions.TABLE).orElse(null);
        List<Map> tableList =
                config.getOptional(DorisSourceOptions.TABLE_LIST)
                        .orElse(Collections.<Map>emptyList());
        if (hasText(singleTable) == !tableList.isEmpty()) {
            throw new IllegalArgumentException(
                    "Exactly one of Doris source table or table_list must be configured");
        }

        List<DorisSourceTableConfig> tables = new ArrayList<DorisSourceTableConfig>();
        if (hasText(singleTable)) {
            String database = requireText(commonDatabase, "database");
            tables.add(
                    new DorisSourceTableConfig(
                            database,
                            singleTable,
                            commonFilter,
                            commonFields,
                            commonTabletSize,
                            commonBatchSize,
                            commonMemLimit));
        } else {
            for (Map item : tableList) {
                String database = firstText(item, "database");
                if (!hasText(database)) {
                    database = commonDatabase;
                }
                database = requireText(database, "table_list[].database");
                String table = requireText(stringValue(item.get("table")), "table_list[].table");
                String filter = firstText(item, "doris.filter.query", "filter_query", "scan_filter");
                if (!hasText(filter)) {
                    filter = commonFilter;
                }
                Object fieldsValue = firstValue(item, "doris.read.field", "read_fields");
                List<String> fields =
                        fieldsValue == null ? commonFields : parseFieldsValue(fieldsValue);
                int tabletSize =
                        intValue(
                                firstValue(item, "doris.request.tablet.size", "request_tablet_size"),
                                commonTabletSize,
                                "table_list[].doris.request.tablet.size");
                int batchSize =
                        intValue(
                                firstValue(item, "doris.batch.size", "scan_batch_rows"),
                                commonBatchSize,
                                "table_list[].doris.batch.size");
                long memLimit =
                        longValue(
                                firstValue(item, "doris.exec.mem.limit", "scan_mem_limit"),
                                commonMemLimit,
                                "table_list[].doris.exec.mem.limit");
                tables.add(
                        new DorisSourceTableConfig(
                                database, table, filter, fields, tabletSize, batchSize, memLimit));
            }
        }

        int connectTimeoutMs = config.get(DorisSourceOptions.REQUEST_CONNECT_TIMEOUT_MS);
        int readTimeoutMs = config.get(DorisSourceOptions.REQUEST_READ_TIMEOUT_MS);
        int queryTimeoutSec = config.get(DorisSourceOptions.REQUEST_QUERY_TIMEOUT_SEC);
        int retries = config.get(DorisSourceOptions.REQUEST_RETRIES);

        if (queryPort <= 0 || queryPort > 65535) {
            throw new IllegalArgumentException("query-port must be between 1 and 65535");
        }
        validatePositive(commonTabletSize, "doris.request.tablet.size");
        validatePositive(commonBatchSize, "doris.batch.size");
        if (commonMemLimit <= 0L) {
            throw new IllegalArgumentException("doris.exec.mem.limit must be greater than 0");
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("doris.request.connect.timeout.ms must be greater than 0");
        }
        if (readTimeoutMs <= 0) {
            throw new IllegalArgumentException("doris.request.read.timeout.ms must be greater than 0");
        }
        if (queryTimeoutSec == 0 || queryTimeoutSec < -1) {
            throw new IllegalArgumentException(
                    "doris.request.query.timeout.s must be -1 or greater than 0");
        }
        if (retries < 0) {
            throw new IllegalArgumentException("doris.request.retries must not be negative");
        }

        return new DorisSourceConfig(
                feNodes,
                queryPort,
                username,
                password == null ? "" : password,
                tables,
                connectTimeoutMs,
                readTimeoutMs,
                queryTimeoutSec,
                retries);
    }

    private static void validatePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be greater than 0");
        }
    }

    private static int intValue(Object value, int defaultValue, String name) {
        if (value == null) {
            return defaultValue;
        }
        try {
            int result = value instanceof Number
                    ? ((Number) value).intValue()
                    : Integer.parseInt(String.valueOf(value));
            validatePositive(result, name);
            return result;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer", e);
        }
    }

    private static long longValue(Object value, long defaultValue, String name) {
        if (value == null) {
            return defaultValue;
        }
        try {
            long result = value instanceof Number
                    ? ((Number) value).longValue()
                    : Long.parseLong(String.valueOf(value));
            if (result <= 0L) {
                throw new IllegalArgumentException(name + " must be greater than 0");
            }
            return result;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a long", e);
        }
    }

    private static List<String> parseFeNodes(String value) {
        if (!hasText(value)) {
            throw new IllegalArgumentException("fenodes must contain at least one FE HTTP address");
        }
        List<String> result = new ArrayList<String>();
        for (String part : value.split(",")) {
            String node = requireText(part, "fenodes item");
            while (node.endsWith("/")) {
                node = node.substring(0, node.length() - 1);
            }
            result.add(node);
        }
        return result;
    }

    private static List<String> parseFields(String value) {
        if (!hasText(value)) {
            return Collections.emptyList();
        }
        List<String> fields = new ArrayList<String>();
        for (String field : value.split(",")) {
            fields.add(requireText(field, "doris.read.field item"));
        }
        return fields;
    }

    private static List<String> parseFieldsValue(Object value) {
        if (value instanceof List) {
            List<String> result = new ArrayList<String>();
            for (Object item : (List<?>) value) {
                result.add(requireText(stringValue(item), "read_fields item"));
            }
            return result;
        }
        return parseFields(stringValue(value));
    }

    private static Object firstValue(Map<?, ?> values, String... keys) {
        for (String key : keys) {
            Object value = values.get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static String firstText(Map<?, ?> values, String... keys) {
        return stringValue(firstValue(values, keys));
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static String requireText(String value, String name) {
        if (!hasText(value)) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value.trim();
    }

    public List<String> getFeNodes() { return feNodes; }
    public String getFenodes() { return String.join(",", feNodes); }
    public int getQueryPort() { return queryPort; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public List<DorisSourceTableConfig> getTableConfigs() { return tableConfigs; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public int getReadTimeoutMs() { return readTimeoutMs; }
    public int getQueryTimeoutSec() { return queryTimeoutSec; }
    public int getRequestRetries() { return requestRetries; }
}
