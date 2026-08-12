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

    public StreamLoadResponse load(String data) throws IOException {
        return load(data, generateLabel());
    }

    public StreamLoadResponse load(String data, String label) throws IOException {
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

        String credentials = config.getUsername() + ":" + config.getPassword();
        String encoded = Base64.getEncoder().encodeToString(
                credentials.getBytes(StandardCharsets.UTF_8));
        headers.put("Authorization", "Basic " + encoded);

        headers.put("label", label);

        DorisLoadFormat format = config.getLoadFormat();
        if (format == DorisLoadFormat.JSON) {
            headers.put("format", "json");
            headers.put("read_json_by_line", "true");
        } else {
            headers.put("format", "csv");
            headers.put("column_separator", config.getCsvColumnSeparator());
        }

        if (config.isEnableDelete()) {
            headers.put("merge_type", "DELETE");
        }

        if (config.isEnable2pc()) {
            headers.put("two_phase_commit", "true");
        }

        for (Map.Entry<String, String> entry : config.getDorisConfig().entrySet()) {
            headers.put(entry.getKey(), entry.getValue());
        }

        headers.put("Expect", "100-continue");

        return headers;
    }

    private String generateLabel() {
        return config.getSinkLabelPrefix() + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    public void commitTransaction(String txnId) throws IOException {
        executeTransactionOperation(txnId, "commit");
    }

    public void abortTransaction(String txnId) throws IOException {
        executeTransactionOperation(txnId, "abort");
    }

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

        public static StreamLoadResponse parse(int httpStatus, String body) {
            String status = extractJsonString(body, "Status");
            String message = extractJsonString(body, "Message");
            long totalRows = extractJsonLong(body, "NumberTotalRows");
            long loadedRows = extractJsonLong(body, "NumberLoadedRows");
            long filteredRows = extractJsonLong(body, "NumberFilteredRows");
            long unselectedRows = extractJsonLong(body, "NumberUnselectedRows");
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
