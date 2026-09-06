package com.link.up.connector.clickhouse.config;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Immutable runtime configuration for the bounded ClickHouse Sink. */
public final class ClickHouseSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final String username;
    private final String password;
    private final String database;
    private final String table;
    private final int batchSize;
    private final String serverTimeZone;
    private final Map<String, String> clientConfig;

    private ClickHouseSinkConfig(
            String host,
            String username,
            String password,
            String database,
            String table,
            int batchSize,
            String serverTimeZone,
            Map<String, String> clientConfig) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.database = database;
        this.table = table;
        this.batchSize = batchSize;
        this.serverTimeZone = serverTimeZone;
        this.clientConfig =
                Collections.unmodifiableMap(new LinkedHashMap<String, String>(clientConfig));
    }

    public static ClickHouseSinkConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");

        String host = normalizeHost(requireText(config.get(ClickHouseSinkOptions.HOST), "host"));
        String username = requireText(config.get(ClickHouseSinkOptions.USERNAME), "username");
        String password = config.get(ClickHouseSinkOptions.PASSWORD);
        String database = requireText(config.get(ClickHouseSinkOptions.DATABASE), "database");
        String table = requireText(config.get(ClickHouseSinkOptions.TABLE), "table");
        int batchSize = config.get(ClickHouseSinkOptions.BATCH_SIZE);
        if (batchSize <= 0) {
            throw new IllegalArgumentException("sink.batch_size must be greater than 0");
        }
        String serverTimeZone =
                normalize(config.getOptional(ClickHouseSinkOptions.SERVER_TIME_ZONE).orElse(null));
        Map<String, String> rawClientConfig =
                config.getOptional(ClickHouseSinkOptions.CLICKHOUSE_CONFIG)
                        .orElse(Collections.<String, String>emptyMap());
        Map<String, String> clientConfig = normalizeClientConfig(rawClientConfig);
        validateSafeAcknowledgement(clientConfig);

        return new ClickHouseSinkConfig(
                host,
                username,
                password == null ? "" : password,
                database,
                table,
                batchSize,
                serverTimeZone,
                clientConfig);
    }

    private static String normalizeHost(String value) {
        if (value.indexOf(',') >= 0) {
            throw new IllegalArgumentException(
                    "ClickHouse bounded Sink requires exactly one host. Use one load-balancer, one Distributed-table endpoint, or one explicit node.");
        }
        String host = value.trim();
        while (host.endsWith("/")) {
            host = host.substring(0, host.length() - 1);
        }
        if (host.startsWith("jdbc:")) {
            throw new IllegalArgumentException(
                    "ClickHouse sink host must be an HTTP endpoint, not a JDBC URL: " + value);
        }
        if (host.indexOf('?') >= 0) {
            throw new IllegalArgumentException(
                    "ClickHouse sink host must not contain query parameters; use clickhouse.config instead");
        }
        return host;
    }

    private static Map<String, String> normalizeClientConfig(Map<String, String> input) {
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (Map.Entry<String, String> entry : input.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            String key = entry.getKey().trim();
            if (key.isEmpty()) {
                continue;
            }
            result.put(key, entry.getValue().trim());
        }
        return result;
    }

    private static void validateSafeAcknowledgement(Map<String, String> clientConfig) {
        for (Map.Entry<String, String> entry : clientConfig.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if ("async_insert".equals(key) && isTrue(entry.getValue())) {
                throw new IllegalArgumentException(
                        "ClickHouse bounded Sink owns client-side batching and requires async_insert=0 so executeBatch is the remote durability boundary");
            }
            if ("wait_for_async_insert".equals(key) && isFalse(entry.getValue())) {
                throw new IllegalArgumentException(
                        "wait_for_async_insert=0 is not allowed because ClickHouse may acknowledge before buffered data is flushed");
            }
        }
    }

    private static boolean isTrue(String value) {
        if (value == null) {
            return false;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return "1".equals(normalized)
                || "true".equals(normalized)
                || "yes".equals(normalized)
                || "on".equals(normalized);
    }

    private static boolean isFalse(String value) {
        if (value == null) {
            return false;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return "0".equals(normalized)
                || "false".equals(normalized)
                || "no".equals(normalized)
                || "off".equals(normalized);
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

    public String getHost() {
        return host;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public TablePath getTargetPath() {
        return TablePath.of(database, table);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getServerTimeZone() {
        return serverTimeZone;
    }

    public Map<String, String> getClientConfig() {
        return clientConfig;
    }
}
