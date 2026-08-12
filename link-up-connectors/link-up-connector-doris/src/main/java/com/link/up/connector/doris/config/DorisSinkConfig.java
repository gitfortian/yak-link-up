package com.link.up.connector.doris.config;

import com.link.up.api.configuration.ReadonlyConfig;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Doris Sink 解析后的不可变配置。
 */
public final class DorisSinkConfig {

    private final String fenodes;
    private final String benodes;
    private final boolean directToBe;
    private final int queryPort;
    private final String username;
    private final String password;
    private final String database;
    private final String table;
    private final String sinkLabelPrefix;
    private final boolean enable2pc;
    private final boolean enableDelete;
    private final int checkIntervalMs;
    private final int maxRetries;
    private final int bufferSize;
    private final int bufferCount;
    private final int batchSize;
    private final DorisLoadFormat loadFormat;
    private final String csvColumnSeparator;
    private final Map<String, String> dorisConfig;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;

    private DorisSinkConfig(Builder b) {
        this.fenodes = b.fenodes;
        this.benodes = b.benodes;
        this.directToBe = b.directToBe;
        this.queryPort = b.queryPort;
        this.username = b.username;
        this.password = b.password;
        this.database = b.database;
        this.table = b.table;
        this.sinkLabelPrefix = b.sinkLabelPrefix;
        this.enable2pc = b.enable2pc;
        this.enableDelete = b.enableDelete;
        this.checkIntervalMs = b.checkIntervalMs;
        this.maxRetries = b.maxRetries;
        this.bufferSize = b.bufferSize;
        this.bufferCount = b.bufferCount;
        this.batchSize = b.batchSize;
        this.loadFormat = b.loadFormat;
        this.csvColumnSeparator = b.csvColumnSeparator;
        this.dorisConfig = b.dorisConfig;
        this.connectTimeoutMs = b.connectTimeoutMs;
        this.socketTimeoutMs = b.socketTimeoutMs;
    }

    public static DorisSinkConfig of(ReadonlyConfig options) {
        Objects.requireNonNull(options, "options must not be null");

        String fenodes = options.get(DorisSinkOptions.FENODES);
        if (fenodes == null || fenodes.trim().isEmpty()) {
            throw new IllegalArgumentException("Doris Sink 'fenodes' must not be blank");
        }
        String username = options.get(DorisSinkOptions.USERNAME);
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalArgumentException("Doris Sink 'username' must not be blank");
        }
        String database = options.get(DorisSinkOptions.DATABASE);
        if (database == null || database.trim().isEmpty()) {
            throw new IllegalArgumentException("Doris Sink 'database' must not be blank");
        }
        String table = options.get(DorisSinkOptions.TABLE);
        if (table == null || table.trim().isEmpty()) {
            throw new IllegalArgumentException("Doris Sink 'table' must not be blank");
        }
        String labelPrefix = options.get(DorisSinkOptions.SINK_LABEL_PREFIX);
        if (labelPrefix == null || labelPrefix.trim().isEmpty()) {
            throw new IllegalArgumentException("Doris Sink 'sink.label-prefix' must not be blank");
        }

        return new Builder()
                .fenodes(fenodes.trim())
                .benodes(options.get(DorisSinkOptions.BENODES))
                .directToBe(options.get(DorisSinkOptions.DIRECT_TO_BE))
                .queryPort(options.get(DorisSinkOptions.QUERY_PORT))
                .username(username.trim())
                .password(options.get(DorisSinkOptions.PASSWORD))
                .database(database.trim())
                .table(table.trim())
                .sinkLabelPrefix(labelPrefix.trim())
                .enable2pc(options.get(DorisSinkOptions.SINK_ENABLE_2PC))
                .enableDelete(options.get(DorisSinkOptions.SINK_ENABLE_DELETE))
                .checkIntervalMs(options.get(DorisSinkOptions.SINK_CHECK_INTERVAL_MS))
                .maxRetries(options.get(DorisSinkOptions.SINK_MAX_RETRIES))
                .bufferSize(options.get(DorisSinkOptions.SINK_BUFFER_SIZE))
                .bufferCount(options.get(DorisSinkOptions.SINK_BUFFER_COUNT))
                .batchSize(options.get(DorisSinkOptions.DORIS_BATCH_SIZE))
                .loadFormat(options.get(DorisSinkOptions.LOAD_FORMAT))
                .csvColumnSeparator(options.get(DorisSinkOptions.CSV_COLUMN_SEPARATOR))
                .dorisConfig(copyMap(options.get(DorisSinkOptions.DORIS_CONFIG)))
                .connectTimeoutMs(options.get(DorisSinkOptions.CONNECT_TIMEOUT_MS))
                .socketTimeoutMs(options.get(DorisSinkOptions.SOCKET_TIMEOUT_MS))
                .build();
    }

    private static <K, V> Map<K, V> copyMap(Map<K, V> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    /**
     * 解析 FE 节点列表。
     */
    public List<String> getFeNodeList() {
        return parseNodes(fenodes);
    }

    /**
     * 解析 BE 节点列表。
     */
    public List<String> getBeNodeList() {
        if (benodes == null || benodes.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return parseNodes(benodes);
    }

    private static List<String> parseNodes(String nodes) {
        String[] parts = nodes.split(",");
        List<String> result = new java.util.ArrayList<>(parts.length);
        for (String p : parts) {
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }

    // ── Getters ──────────────────────────────────────────

    public String getFenodes() { return fenodes; }
    public String getBenodes() { return benodes; }
    public boolean isDirectToBe() { return directToBe; }
    public int getQueryPort() { return queryPort; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public String getDatabase() { return database; }
    public String getTable() { return table; }
    public String getSinkLabelPrefix() { return sinkLabelPrefix; }
    public boolean isEnable2pc() { return enable2pc; }
    public boolean isEnableDelete() { return enableDelete; }
    public int getCheckIntervalMs() { return checkIntervalMs; }
    public int getMaxRetries() { return maxRetries; }
    public int getBufferSize() { return bufferSize; }
    public int getBufferCount() { return bufferCount; }
    public int getBatchSize() { return batchSize; }
    public DorisLoadFormat getLoadFormat() { return loadFormat; }
    public String getCsvColumnSeparator() { return csvColumnSeparator; }
    public Map<String, String> getDorisConfig() { return dorisConfig; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public int getSocketTimeoutMs() { return socketTimeoutMs; }

    public static final class Builder {
        private String fenodes;
        private String benodes;
        private boolean directToBe = false;
        private int queryPort = 9030;
        private String username;
        private String password;
        private String database;
        private String table;
        private String sinkLabelPrefix;
        private boolean enable2pc = false;
        private boolean enableDelete = false;
        private int checkIntervalMs = 10000;
        private int maxRetries = 3;
        private int bufferSize = 262144;
        private int bufferCount = 3;
        private int batchSize = 1024;
        private DorisLoadFormat loadFormat = DorisLoadFormat.JSON;
        private String csvColumnSeparator = ",";
        private Map<String, String> dorisConfig = Collections.emptyMap();
        private int connectTimeoutMs = 30000;
        private int socketTimeoutMs = 300000;

        public Builder fenodes(String v) { this.fenodes = v; return this; }
        public Builder benodes(String v) { this.benodes = v; return this; }
        public Builder directToBe(boolean v) { this.directToBe = v; return this; }
        public Builder queryPort(int v) { this.queryPort = v; return this; }
        public Builder username(String v) { this.username = v; return this; }
        public Builder password(String v) { this.password = v; return this; }
        public Builder database(String v) { this.database = v; return this; }
        public Builder table(String v) { this.table = v; return this; }
        public Builder sinkLabelPrefix(String v) { this.sinkLabelPrefix = v; return this; }
        public Builder enable2pc(boolean v) { this.enable2pc = v; return this; }
        public Builder enableDelete(boolean v) { this.enableDelete = v; return this; }
        public Builder checkIntervalMs(int v) { this.checkIntervalMs = v; return this; }
        public Builder maxRetries(int v) { this.maxRetries = v; return this; }
        public Builder bufferSize(int v) { this.bufferSize = v; return this; }
        public Builder bufferCount(int v) { this.bufferCount = v; return this; }
        public Builder batchSize(int v) { this.batchSize = v; return this; }
        public Builder loadFormat(DorisLoadFormat v) { this.loadFormat = v; return this; }
        public Builder csvColumnSeparator(String v) { this.csvColumnSeparator = v; return this; }
        public Builder dorisConfig(Map<String, String> v) { this.dorisConfig = v; return this; }
        public Builder connectTimeoutMs(int v) { this.connectTimeoutMs = v; return this; }
        public Builder socketTimeoutMs(int v) { this.socketTimeoutMs = v; return this; }

        public DorisSinkConfig build() {
            return new DorisSinkConfig(this);
        }
    }
}
