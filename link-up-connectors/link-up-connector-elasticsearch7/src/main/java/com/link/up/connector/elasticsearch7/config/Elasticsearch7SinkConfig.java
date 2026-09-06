package com.link.up.connector.elasticsearch7.config;

import com.link.up.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Immutable configuration for the bounded Elasticsearch 7 Sink. */
public final class Elasticsearch7SinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> hosts;
    private final String username;
    private final String password;
    private final String index;
    private final String documentIdField;
    private final int batchSize;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final long maxRetryBackoffMs;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;

    private Elasticsearch7SinkConfig(
            List<String> hosts,
            String username,
            String password,
            String index,
            String documentIdField,
            int batchSize,
            int maxRetries,
            long retryBackoffMs,
            long maxRetryBackoffMs,
            int connectTimeoutMs,
            int socketTimeoutMs) {
        this.hosts = Collections.unmodifiableList(new ArrayList<String>(hosts));
        this.username = username;
        this.password = password;
        this.index = index;
        this.documentIdField = documentIdField;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
        this.maxRetryBackoffMs = maxRetryBackoffMs;
        this.connectTimeoutMs = connectTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
    }

    public static Elasticsearch7SinkConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");

        List<String> hosts =
                normalizeHosts(
                        config.getOptional(Elasticsearch7SinkOptions.HOSTS)
                                .orElse(Collections.<String>emptyList()));
        String username = normalize(config.get(Elasticsearch7SinkOptions.USERNAME));
        String password = config.get(Elasticsearch7SinkOptions.PASSWORD);
        String index = requireConcreteIndex(config.get(Elasticsearch7SinkOptions.INDEX));
        String documentIdField =
                normalize(config.getOptional(Elasticsearch7SinkOptions.DOCUMENT_ID_FIELD).orElse(null));
        int batchSize = config.get(Elasticsearch7SinkOptions.BATCH_SIZE);
        int maxRetries = config.get(Elasticsearch7SinkOptions.MAX_RETRIES);
        long retryBackoffMs = config.get(Elasticsearch7SinkOptions.RETRY_BACKOFF_MS);
        long maxRetryBackoffMs = config.get(Elasticsearch7SinkOptions.MAX_RETRY_BACKOFF_MS);
        int connectTimeoutMs = config.get(Elasticsearch7SinkOptions.CONNECT_TIMEOUT_MS);
        int socketTimeoutMs = config.get(Elasticsearch7SinkOptions.SOCKET_TIMEOUT_MS);

        validatePositive(batchSize, "batch_size");
        if (maxRetries < 0) {
            throw new IllegalArgumentException("max_retries must not be negative");
        }
        validatePositive(retryBackoffMs, "retry_backoff_ms");
        validatePositive(maxRetryBackoffMs, "max_retry_backoff_ms");
        if (maxRetryBackoffMs < retryBackoffMs) {
            throw new IllegalArgumentException(
                    "max_retry_backoff_ms must be greater than or equal to retry_backoff_ms");
        }
        validatePositive(connectTimeoutMs, "connect_timeout_ms");
        validatePositive(socketTimeoutMs, "socket_timeout_ms");

        return new Elasticsearch7SinkConfig(
                hosts,
                username == null ? "" : username,
                password == null ? "" : password,
                index,
                documentIdField,
                batchSize,
                maxRetries,
                retryBackoffMs,
                maxRetryBackoffMs,
                connectTimeoutMs,
                socketTimeoutMs);
    }

    private static List<String> normalizeHosts(List<String> values) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("hosts must contain at least one Elasticsearch node");
        }
        List<String> result = new ArrayList<String>();
        for (String value : values) {
            String host = normalize(value);
            if (host == null) {
                throw new IllegalArgumentException("hosts values must not be blank");
            }
            validateHost(host);
            result.add(stripTrailingSlash(host));
        }
        return result;
    }

    private static void validateHost(String value) {
        final URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException failure) {
            throw new IllegalArgumentException("Invalid Elasticsearch host: " + value, failure);
        }
        String scheme = uri.getScheme();
        if (scheme == null
                || (!("http".equalsIgnoreCase(scheme)) && !("https".equalsIgnoreCase(scheme)))) {
            throw new IllegalArgumentException("Elasticsearch host must use http or https: " + value);
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Elasticsearch host must include a hostname: " + value);
        }
        String path = uri.getPath();
        if (path != null && !path.isEmpty() && !"/".equals(path)) {
            throw new IllegalArgumentException("Elasticsearch host must not include a path: " + value);
        }
        if (uri.getQuery() != null || uri.getFragment() != null || uri.getUserInfo() != null) {
            throw new IllegalArgumentException(
                    "Elasticsearch host must not include credentials, query, or fragment: " + value);
        }
    }

    private static String requireConcreteIndex(String value) {
        String index = normalize(value);
        if (index == null) {
            throw new IllegalArgumentException("index must not be empty");
        }
        if (index.indexOf(',') >= 0 || index.indexOf('*') >= 0 || index.indexOf('?') >= 0) {
            throw new IllegalArgumentException(
                    "Stage 2 Elasticsearch 7 Sink requires one existing concrete index or single-index alias; wildcards and multi-index expressions are not supported: "
                            + index);
        }
        return index;
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static void validatePositive(long value, String name) {
        if (value <= 0L) {
            throw new IllegalArgumentException(name + " must be greater than 0");
        }
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
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

    public String getIndex() {
        return index;
    }

    public String getDocumentIdField() {
        return documentIdField;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public long getMaxRetryBackoffMs() {
        return maxRetryBackoffMs;
    }

    public long retryBackoffMs(int retryNumber) {
        if (retryNumber <= 0) {
            return retryBackoffMs;
        }
        long value = retryBackoffMs;
        for (int index = 0; index < retryNumber && value < maxRetryBackoffMs; index++) {
            value = Math.min(maxRetryBackoffMs, value > Long.MAX_VALUE / 2 ? maxRetryBackoffMs : value * 2L);
        }
        return value;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }
}
