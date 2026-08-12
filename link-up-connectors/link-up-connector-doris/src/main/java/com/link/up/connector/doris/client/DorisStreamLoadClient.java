package com.link.up.connector.doris.client;

import com.link.up.connector.doris.config.DorisLoadFormat;
import com.link.up.connector.doris.config.DorisSinkConfig;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Doris Stream Load 客户端。
 *
 * <p>通过 HTTP PUT 将数据批量写入 Doris，
 * 支持 JSON 和 CSV 两种数据格式。
 */
public final class DorisStreamLoadClient implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(DorisStreamLoadClient.class);

    private static final MediaType TEXT_PLAIN =
            MediaType.parse("text/plain; charset=utf-8");

    private final DorisSinkConfig config;
    private final OkHttpClient httpClient;
    private final AtomicInteger feRoundRobin = new AtomicInteger(0);

    public DorisStreamLoadClient(DorisSinkConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(config.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                .followRedirects(false)
                .build();
    }

    /**
     * 执行一次 Stream Load。
     *
     * @param data Stream Load 数据内容（JSON lines 或 CSV）
     * @return Stream Load 响应结果
     */
    public StreamLoadResponse load(String data) throws IOException {
        return load(data, generateLabel());
    }

    /**
     * 执行一次 Stream Load（指定 label）。
     *
     * <p><b>精确一次性保证：</b>重试时复用同一个 label。
     * Doris 对相同 label 的 Stream Load 请求会做幂等校验，
     * 避免网络超时重试导致的数据重复写入。
     */
    public StreamLoadResponse load(String data, String label) throws IOException {
        // 重试时复用同一个 label，保证幂等性
        // 如果 Doris 已处理该 label 的请求，重复提交会返回 Label Already Exists 错误
        // 而不是产生重复数据
        IOException lastException = null;
        int maxAttempts = Math.max(1, config.getMaxRetries() + 1);

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            String targetNode = selectTargetNode();
            String url = buildStreamLoadUrl(targetNode);

            LOG.debug("Stream Load to {}, label={}, attempt={}/{}", url, label, attempt, maxAttempts);

            Map<String, String> headers = buildHeaders(label);

            Request request = new Request.Builder()
                    .url(url)
                    .put(RequestBody.create(
                            data.getBytes(StandardCharsets.UTF_8), TEXT_PLAIN))
                    .build();

            for (Map.Entry<String, String> entry : headers.entrySet()) {
                request = request.newBuilder()
                        .header(entry.getKey(), entry.getValue())
                        .build();
            }

            try {
                return doLoad(request);
            } catch (IOException e) {
                lastException = e;
                if (attempt < maxAttempts) {
                    LOG.warn("Stream Load failed (attempt {}/{}), label={}, retrying: {}",
                            attempt, maxAttempts, label, e.getMessage());
                }
            }
        }

        throw lastException;
    }

    private StreamLoadResponse doLoad(Request request) throws IOException {
        try (Response response = httpClient.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";

            // 处理 307 redirect
            if (response.code() == 307) {
                String redirectUrl = response.header("Location");
                if (redirectUrl != null && !redirectUrl.isEmpty()) {
                    LOG.debug("Stream Load redirect to {}", redirectUrl);
                    return doRedirect(request, redirectUrl);
                }
            }

            return StreamLoadResponse.parse(response.code(), body);
        }
    }

    private StreamLoadResponse doRedirect(Request originalRequest, String redirectUrl)
            throws IOException {

        Request.Builder redirectBuilder = originalRequest.newBuilder()
                .url(redirectUrl);

        try (Response response = httpClient.newCall(redirectBuilder.build()).execute()) {
            String body = response.body() != null ? response.body().string() : "";
            return StreamLoadResponse.parse(response.code(), body);
        }
    }

    private String selectTargetNode() {
        if (config.isDirectToBe()) {
            List<String> beNodes = config.getBeNodeList();
            if (!beNodes.isEmpty()) {
                int idx = Math.abs(feRoundRobin.getAndIncrement() % beNodes.size());
                return beNodes.get(idx);
            }
            LOG.warn("direct_to_be=true but no benodes configured, falling back to fenodes");
        }

        List<String> feNodes = config.getFeNodeList();
        int idx = Math.abs(feRoundRobin.getAndIncrement() % feNodes.size());
        return feNodes.get(idx);
    }

    private String buildStreamLoadUrl(String node) {
        String host = node;
        if (!host.startsWith("http://") && !host.startsWith("https://")) {
            host = "http://" + host;
        }
        return host + "/api/" + config.getDatabase() + "/" + config.getTable() + "/_stream_load";
    }

    private Map<String, String> buildHeaders(String label) {
        Map<String, String> headers = new LinkedHashMap<>();

        // Basic Auth
        String credentials = config.getUsername() + ":" + config.getPassword();
        String encoded = Base64.getEncoder().encodeToString(
                credentials.getBytes(StandardCharsets.UTF_8));
        headers.put("Authorization", "Basic " + encoded);

        // Label
        headers.put("label", label);

        // 数据格式
        DorisLoadFormat format = config.getLoadFormat();
        if (format == DorisLoadFormat.JSON) {
            headers.put("format", "json");
            headers.put("read_json_by_line", "true");
        } else {
            headers.put("format", "csv");
            headers.put("column_separator", config.getCsvColumnSeparator());
        }

        // 启用删除
        if (config.isEnableDelete()) {
            headers.put("merge_type", "DELETE");
        }

        // 2PC
        if (config.isEnable2pc()) {
            headers.put("two_phase_commit", "true");
        }

        // 透传 doris.config
        for (Map.Entry<String, String> entry : config.getDorisConfig().entrySet()) {
            headers.put(entry.getKey(), entry.getValue());
        }

        // Expect: 100-continue（大数据量时避免发送失败请求体）
        headers.put("Expect", "100-continue");

        return headers;
    }

    private String generateLabel() {
        return config.getSinkLabelPrefix() + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    /**
     * 提交一个 2PC 预提交事务。
     *
     * <p>Doris 2PC API：
     * {@code PUT /api/{db}/{table}/_stream_load_2pc}
     * Body: {@code {"txn_id": <id>, "operation": "commit"}}
     *
     * @param txnId Stream Load 返回的事务 ID
     */
    public void commitTransaction(String txnId) throws IOException {
        executeTransactionOperation(txnId, "commit");
    }

    /**
     * 回滚一个 2PC 预提交事务。
     *
     * @param txnId Stream Load 返回的事务 ID
     */
    public void abortTransaction(String txnId) throws IOException {
        executeTransactionOperation(txnId, "abort");
    }

    /**
     * 批量提交事务。
     *
     * @param txnIds 事务 ID 列表
     */
    public void commitTransactions(List<String> txnIds) throws IOException {
        IOException lastException = null;
        for (String txnId : txnIds) {
            try {
                commitTransaction(txnId);
            } catch (IOException e) {
                LOG.error("Failed to commit txnId={}: {}", txnId, e.getMessage());
                if (lastException == null) {
                    lastException = e;
                } else {
                    lastException.addSuppressed(e);
                }
            }
        }
        if (lastException != null) {
            throw lastException;
        }
    }

    /**
     * 批量回滚事务。
     *
     * @param txnIds 事务 ID 列表
     */
    public void abortTransactions(List<String> txnIds) {
        for (String txnId : txnIds) {
            try {
                abortTransaction(txnId);
            } catch (IOException e) {
                LOG.warn("Failed to abort txnId={}: {}", txnId, e.getMessage());
            }
        }
    }

    private void executeTransactionOperation(String txnId, String operation) throws IOException {
        String targetNode = selectTargetNode();
        String url = build2pcUrl(targetNode);

        String body = "{\"txn_id\": " + txnId + ", \"operation\": \"" + operation + "\"}";

        LOG.debug("2PC {} txnId={} to {}", operation, txnId, url);

        String credentials = config.getUsername() + ":" + config.getPassword();
        String encoded = Base64.getEncoder().encodeToString(
                credentials.getBytes(StandardCharsets.UTF_8));

        Request request = new Request.Builder()
                .url(url)
                .put(RequestBody.create(
                        body.getBytes(StandardCharsets.UTF_8), TEXT_PLAIN))
                .header("Authorization", "Basic " + encoded)
                .header("Content-Type", "application/json")
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";

            if (!response.isSuccessful()) {
                throw new IOException("Doris 2PC " + operation + " failed: txnId=" + txnId
                        + ", httpStatus=" + response.code()
                        + ", body=" + responseBody);
            }

            // 检查响应中的 Status 字段
            String status = StreamLoadResponse.extractJsonStringStatic(responseBody, "status");
            if (status != null && !"OK".equalsIgnoreCase(status) && !"Success".equalsIgnoreCase(status)) {
                String msg = StreamLoadResponse.extractJsonStringStatic(responseBody, "message");
                throw new IOException("Doris 2PC " + operation + " failed: txnId=" + txnId
                        + ", status=" + status + ", message=" + msg);
            }

            LOG.debug("2PC {} txnId={} success", operation, txnId);
        }
    }

    private String build2pcUrl(String node) {
        String host = node;
        if (!host.startsWith("http://") && !host.startsWith("https://")) {
            host = "http://" + host;
        }
        return host + "/api/" + config.getDatabase() + "/" + config.getTable() + "/_stream_load_2pc";
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    /**
     * Stream Load 响应结果。
     */
    public static final class StreamLoadResponse {
        private final int httpStatus;
        private final String body;
        private final String status;
        private final String message;
        private final long numberTotalRows;
        private final long numberLoadedRows;
        private final long numberFilteredRows;
        private final long numberUnselectedRows;
        private final String txnId;
        private final String label;
        private final String txnState;

        private StreamLoadResponse(int httpStatus, String body, String status, String message,
                                   long totalRows, long loadedRows, long filteredRows,
                                   long unselectedRows, String txnId, String label, String txnState) {
            this.httpStatus = httpStatus;
            this.body = body;
            this.status = status;
            this.message = message;
            this.numberTotalRows = totalRows;
            this.numberLoadedRows = loadedRows;
            this.numberFilteredRows = filteredRows;
            this.numberUnselectedRows = unselectedRows;
            this.txnId = txnId;
            this.label = label;
            this.txnState = txnState;
        }

        /**
         * 解析 Stream Load 响应 JSON。
         *
         * <p>Doris Stream Load 返回格式：
         * <pre>
         * {
         *   "Status": "Success",
         *   "Message": "...",
         *   "NumberTotalRows": 100,
         *   "NumberLoadedRows": 100,
         *   "TxnId": 12345,
         *   "Label": "...",
         *   "TxnState": "PREPARE",
         *   ...
         * }
         * </pre>
         *
         * <p>注意：TxnId 在 Doris 响应中是数字类型（无引号），
         * 需要同时支持字符串和数字两种解析方式。
         */
        public static StreamLoadResponse parse(int httpStatus, String body) {
            String status = extractJsonString(body, "Status");
            String message = extractJsonString(body, "Message");
            long totalRows = extractJsonLong(body, "NumberTotalRows");
            long loadedRows = extractJsonLong(body, "NumberLoadedRows");
            long filteredRows = extractJsonLong(body, "NumberFilteredRows");
            long unselectedRows = extractJsonLong(body, "NumberUnselectedRows");
            // TxnId 在 Doris 响应中是数字，用 extractJsonLong 解析后转 String
            long txnIdLong = extractJsonLong(body, "TxnId");
            String txnId = txnIdLong >= 0 ? String.valueOf(txnIdLong) : extractJsonString(body, "TxnId");
            String label = extractJsonString(body, "Label");
            String txnState = extractJsonString(body, "TxnState");

            return new StreamLoadResponse(httpStatus, body, status, message,
                    totalRows, loadedRows, filteredRows, unselectedRows,
                    txnId, label, txnState);
        }

        public boolean isSuccess() {
            return "Success".equalsIgnoreCase(status)
                    || "OK".equalsIgnoreCase(status);
        }

        public boolean isPrepared() {
            return "PREPARE".equalsIgnoreCase(txnState);
        }

        public void checkSuccess() throws IOException {
            if (!isSuccess()) {
                throw new IOException("Doris Stream Load failed: status=" + status
                        + ", message=" + message
                        + ", httpStatus=" + httpStatus
                        + ", body=" + body);
            }
        }

        // ── Getters ──────────────────────────────────────

        public int getHttpStatus() { return httpStatus; }
        public String getBody() { return body; }
        public String getStatus() { return status; }
        public String getMessage() { return message; }
        public long getNumberTotalRows() { return numberTotalRows; }
        public long getNumberLoadedRows() { return numberLoadedRows; }
        public long getNumberFilteredRows() { return numberFilteredRows; }
        public long getNumberUnselectedRows() { return numberUnselectedRows; }
        public String getTxnId() { return txnId; }
        public String getLabel() { return label; }
        public String getTxnState() { return txnState; }

        // ── JSON 解析工具 ──────────────────────────────────

        static String extractJsonStringStatic(String json, String key) {
            return extractJsonString(json, key);
        }

        private static String extractJsonString(String json, String key) {
            if (json == null || key == null) return null;
            String searchKey = "\"" + key + "\"";
            int idx = json.indexOf(searchKey);
            if (idx < 0) return null;
            int colonIdx = json.indexOf(':', idx + searchKey.length());
            if (colonIdx < 0) return null;
            int startQuote = json.indexOf('"', colonIdx + 1);
            if (startQuote < 0) return null;
            int endQuote = json.indexOf('"', startQuote + 1);
            if (endQuote < 0) return null;
            return json.substring(startQuote + 1, endQuote);
        }

        private static long extractJsonLong(String json, String key) {
            if (json == null || key == null) return -1;
            String searchKey = "\"" + key + "\"";
            int idx = json.indexOf(searchKey);
            if (idx < 0) return -1;
            int colonIdx = json.indexOf(':', idx + searchKey.length());
            if (colonIdx < 0) return -1;
            int start = colonIdx + 1;
            while (start < json.length() && json.charAt(start) == ' ') start++;
            int end = start;
            while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) end++;
            if (end == start) return -1;
            try {
                return Long.parseLong(json.substring(start, end));
            } catch (NumberFormatException e) {
                return -1;
            }
        }
    }
}
