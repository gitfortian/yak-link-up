package com.link.up.connector.http.catalog;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.connector.http.config.HttpFormat;
import com.link.up.connector.http.config.HttpMethod;
import com.link.up.connector.http.config.HttpSourceConfig;
import com.link.up.connector.http.config.HttpSourceOptions;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * HTTP Catalog 配置。
 *
 * <p>从 {@link ReadonlyConfig} 中解析 Catalog 所需的连接参数，
 * 复用 {@link HttpSourceOptions} 中已定义的 Option 定义。
 */
public final class HttpCatalogConfig {

    private final String url;
    private final HttpMethod method;
    private final Map<String, String> headers;
    private final Map<String, String> params;
    private final String body;
    private final HttpFormat format;
    private final Map<String, Object> schemaFields;
    private final String contentField;
    private final Map<String, Object> jsonField;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;
    private final int retry;
    private final int retryBackoffMultiplierMs;
    private final int retryBackoffMaxMs;
    private final String tableName;

    private HttpCatalogConfig(Builder builder) {
        this.url = Objects.requireNonNull(builder.url, "url must not be null");
        this.method = builder.method;
        this.headers = builder.headers;
        this.params = builder.params;
        this.body = builder.body;
        this.format = builder.format;
        this.schemaFields = builder.schemaFields;
        this.contentField = builder.contentField;
        this.jsonField = builder.jsonField;
        this.connectTimeoutMs = builder.connectTimeoutMs;
        this.socketTimeoutMs = builder.socketTimeoutMs;
        this.retry = builder.retry;
        this.retryBackoffMultiplierMs = builder.retryBackoffMultiplierMs;
        this.retryBackoffMaxMs = builder.retryBackoffMaxMs;
        this.tableName = builder.tableName;
    }

    /**
     * 从 {@link ReadonlyConfig} 构建。
     */
    public static HttpCatalogConfig of(ReadonlyConfig options) {
        Objects.requireNonNull(options, "options must not be null");

        String url = options.get(HttpSourceOptions.URL);
        if (url == null || url.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "HTTP Catalog config requires 'url'");
        }

        return new Builder()
                .url(url.trim())
                .method(options.get(HttpSourceOptions.METHOD))
                .headers(copyMap(options.get(HttpSourceOptions.HEADERS)))
                .params(copyMap(options.get(HttpSourceOptions.PARAMS)))
                .body(options.get(HttpSourceOptions.BODY))
                .format(options.get(HttpSourceOptions.FORMAT))
                .schemaFields(copyMap(options.get(HttpSourceOptions.SCHEMA_FIELDS)))
                .contentField(options.get(HttpSourceOptions.CONTENT_FIELD))
                .jsonField(copyMap(options.get(HttpSourceOptions.JSON_FIELD)))
                .connectTimeoutMs(options.get(HttpSourceOptions.CONNECT_TIMEOUT_MS))
                .socketTimeoutMs(options.get(HttpSourceOptions.SOCKET_TIMEOUT_MS))
                .retry(options.get(HttpSourceOptions.RETRY))
                .retryBackoffMultiplierMs(options.get(HttpSourceOptions.RETRY_BACKOFF_MULTIPLIER_MS))
                .retryBackoffMaxMs(options.get(HttpSourceOptions.RETRY_BACKOFF_MAX_MS))
                .build();
    }

    /**
     * 从已有的 {@link HttpSourceConfig} 构建。
     */
    public static HttpCatalogConfig fromSourceConfig(HttpSourceConfig sourceConfig) {
        Objects.requireNonNull(sourceConfig, "sourceConfig must not be null");

        return new Builder()
                .url(sourceConfig.getUrl())
                .method(sourceConfig.getMethod())
                .headers(sourceConfig.getHeaders())
                .params(sourceConfig.getParams())
                .body(sourceConfig.getBody())
                .format(sourceConfig.getFormat())
                .schemaFields(sourceConfig.getSchemaFields())
                .contentField(sourceConfig.getContentField())
                .jsonField(sourceConfig.getJsonField())
                .connectTimeoutMs(sourceConfig.getConnectTimeoutMs())
                .socketTimeoutMs(sourceConfig.getSocketTimeoutMs())
                .retry(sourceConfig.getRetry())
                .retryBackoffMultiplierMs(sourceConfig.getRetryBackoffMultiplierMs())
                .retryBackoffMaxMs(sourceConfig.getRetryBackoffMaxMs())
                .build();
    }

    private static <K, V> Map<K, V> copyMap(Map<K, V> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    // ── Getters ──────────────────────────────────────────

    public String getUrl() {
        return url;
    }

    public HttpMethod getMethod() {
        return method;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public String getBody() {
        return body;
    }

    public HttpFormat getFormat() {
        return format;
    }

    public Map<String, Object> getSchemaFields() {
        return schemaFields;
    }

    public String getContentField() {
        return contentField;
    }

    public Map<String, Object> getJsonField() {
        return jsonField;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public int getRetry() {
        return retry;
    }

    public int getRetryBackoffMultiplierMs() {
        return retryBackoffMultiplierMs;
    }

    public int getRetryBackoffMaxMs() {
        return retryBackoffMaxMs;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean hasSchemaFields() {
        return schemaFields != null && !schemaFields.isEmpty();
    }

    /**
     * 转换为 {@link HttpSourceConfig}，供 Catalog 内部发起探测请求。
     */
    public HttpSourceConfig toSourceConfig() {
        return new com.link.up.connector.http.config.HttpSourceConfig.Builder()
                .url(url)
                .method(method)
                .headers(headers)
                .params(params)
                .body(body)
                .format(format)
                .schemaFields(schemaFields)
                .contentField(contentField)
                .jsonField(jsonField)
                .connectTimeoutMs(connectTimeoutMs)
                .socketTimeoutMs(socketTimeoutMs)
                .retry(retry)
                .retryBackoffMultiplierMs(retryBackoffMultiplierMs)
                .retryBackoffMaxMs(retryBackoffMaxMs)
                .build();
    }

    public static final class Builder {

        private String url;
        private HttpMethod method = HttpMethod.GET;
        private Map<String, String> headers = Collections.emptyMap();
        private Map<String, String> params = Collections.emptyMap();
        private String body;
        private HttpFormat format = HttpFormat.JSON;
        private Map<String, Object> schemaFields = Collections.emptyMap();
        private String contentField;
        private Map<String, Object> jsonField = Collections.emptyMap();
        private int connectTimeoutMs = 12000;
        private int socketTimeoutMs = 60000;
        private int retry = 3;
        private int retryBackoffMultiplierMs = 100;
        private int retryBackoffMaxMs = 10000;
        private String tableName;

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder method(HttpMethod method) {
            this.method = method;
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Builder params(Map<String, String> params) {
            this.params = params;
            return this;
        }

        public Builder body(String body) {
            this.body = body;
            return this;
        }

        public Builder format(HttpFormat format) {
            this.format = format;
            return this;
        }

        public Builder schemaFields(Map<String, Object> schemaFields) {
            this.schemaFields = schemaFields;
            return this;
        }

        public Builder contentField(String contentField) {
            this.contentField = contentField;
            return this;
        }

        public Builder jsonField(Map<String, Object> jsonField) {
            this.jsonField = jsonField;
            return this;
        }

        public Builder connectTimeoutMs(int connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        public Builder socketTimeoutMs(int socketTimeoutMs) {
            this.socketTimeoutMs = socketTimeoutMs;
            return this;
        }

        public Builder retry(int retry) {
            this.retry = retry;
            return this;
        }

        public Builder retryBackoffMultiplierMs(int retryBackoffMultiplierMs) {
            this.retryBackoffMultiplierMs = retryBackoffMultiplierMs;
            return this;
        }

        public Builder retryBackoffMaxMs(int retryBackoffMaxMs) {
            this.retryBackoffMaxMs = retryBackoffMaxMs;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public HttpCatalogConfig build() {
            return new HttpCatalogConfig(this);
        }
    }
}
