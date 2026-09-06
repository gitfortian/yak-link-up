package com.link.up.connector.elasticsearch7.config;

import com.link.up.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/** Immutable configuration for the bounded Elasticsearch 7 Source. */
public final class Elasticsearch7SourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> hosts;
    private final String username;
    private final String password;
    private final String index;
    private final List<String> sourceFields;
    private final String query;
    private final long scrollTimeMillis;
    private final int scrollSize;
    private final Integer slices;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;

    private Elasticsearch7SourceConfig(
            List<String> hosts,
            String username,
            String password,
            String index,
            List<String> sourceFields,
            String query,
            long scrollTimeMillis,
            int scrollSize,
            Integer slices,
            int connectTimeoutMs,
            int socketTimeoutMs) {
        this.hosts = Collections.unmodifiableList(new ArrayList<String>(hosts));
        this.username = username;
        this.password = password;
        this.index = index;
        this.sourceFields = Collections.unmodifiableList(new ArrayList<String>(sourceFields));
        this.query = query;
        this.scrollTimeMillis = scrollTimeMillis;
        this.scrollSize = scrollSize;
        this.slices = slices;
        this.connectTimeoutMs = connectTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
    }

    public static Elasticsearch7SourceConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");

        List<String> hosts =
                normalizeHosts(
                        config.getOptional(Elasticsearch7SourceOptions.HOSTS)
                                .orElse(Collections.<String>emptyList()));
        String username = normalize(config.get(Elasticsearch7SourceOptions.USERNAME));
        String password = config.get(Elasticsearch7SourceOptions.PASSWORD);
        String index = requireConcreteIndex(config.get(Elasticsearch7SourceOptions.INDEX));
        List<String> sourceFields =
                normalizeSourceFields(
                        config.getOptional(Elasticsearch7SourceOptions.SOURCE)
                                .orElse(Collections.<String>emptyList()));
        String query = normalize(config.get(Elasticsearch7SourceOptions.QUERY));
        long scrollTimeMillis =
                parsePositiveDurationMillis(
                        config.get(Elasticsearch7SourceOptions.SCROLL_TIME),
                        "scroll_time");
        int scrollSize = config.get(Elasticsearch7SourceOptions.SCROLL_SIZE);
        Integer slices = config.getOptional(Elasticsearch7SourceOptions.SLICES).orElse(null);
        int connectTimeoutMs = config.get(Elasticsearch7SourceOptions.CONNECT_TIMEOUT_MS);
        int socketTimeoutMs = config.get(Elasticsearch7SourceOptions.SOCKET_TIMEOUT_MS);

        validatePositive(scrollSize, "scroll_size");
        validatePositive(connectTimeoutMs, "connect_timeout_ms");
        validatePositive(socketTimeoutMs, "socket_timeout_ms");
        if (slices != null) {
            validatePositive(slices, "slices");
        }

        return new Elasticsearch7SourceConfig(
                hosts,
                username == null ? "" : username,
                password == null ? "" : password,
                index,
                sourceFields,
                query,
                scrollTimeMillis,
                scrollSize,
                slices,
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
            throw new IllegalArgumentException("Elasticsearch host must not include credentials, query, or fragment: " + value);
        }
    }

    private static String requireConcreteIndex(String value) {
        String index = normalize(value);
        if (index == null) {
            throw new IllegalArgumentException("index must not be empty");
        }
        if (index.indexOf(',') >= 0 || index.indexOf('*') >= 0 || index.indexOf('?') >= 0) {
            throw new IllegalArgumentException(
                    "Stage 1 Elasticsearch 7 Source requires one concrete index or single-index alias; wildcards and multi-index expressions are not supported: "
                            + index);
        }
        return index;
    }

    private static List<String> normalizeSourceFields(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> unique = new LinkedHashSet<String>();
        for (String value : values) {
            String field = normalize(value);
            if (field == null) {
                throw new IllegalArgumentException("source values must not be blank");
            }
            if (!unique.add(field)) {
                throw new IllegalArgumentException("Duplicate source field: " + field);
            }
        }
        return new ArrayList<String>(unique);
    }

    static long parsePositiveDurationMillis(String value, String name) {
        String text = normalize(value);
        if (text == null) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        String normalized = text.toLowerCase(Locale.ROOT);
        long multiplier;
        String number;
        if (normalized.endsWith("ms")) {
            multiplier = 1L;
            number = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("s")) {
            multiplier = 1000L;
            number = normalized.substring(0, normalized.length() - 1);
        } else if (normalized.endsWith("m")) {
            multiplier = 60_000L;
            number = normalized.substring(0, normalized.length() - 1);
        } else if (normalized.endsWith("h")) {
            multiplier = 3_600_000L;
            number = normalized.substring(0, normalized.length() - 1);
        } else {
            throw new IllegalArgumentException(name + " must use ms, s, m, or h: " + value);
        }
        final long amount;
        try {
            amount = Long.parseLong(number.trim());
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(name + " must be a positive duration: " + value, failure);
        }
        if (amount <= 0L || amount > Long.MAX_VALUE / multiplier) {
            throw new IllegalArgumentException(name + " must be a positive duration: " + value);
        }
        return amount * multiplier;
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static void validatePositive(int value, String name) {
        if (value <= 0) {
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

    public List<String> getSourceFields() {
        return sourceFields;
    }

    public String getQuery() {
        return query;
    }

    public long getScrollTimeMillis() {
        return scrollTimeMillis;
    }

    public int getScrollSize() {
        return scrollSize;
    }

    public Integer getSlices() {
        return slices;
    }

    public int resolveSlices(int parallelism) {
        validatePositive(parallelism, "parallelism");
        return slices == null ? parallelism : slices;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }
}
