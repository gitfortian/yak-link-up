package com.link.up.connector.jdbc.core.dialect.hana;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

/** SAP HANA JDBC URL helpers for the bounded JDBC connector. */
public final class HanaJdbcUrl {

    private static final String PREFIX = "jdbc:sap://";

    private HanaJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim().toLowerCase(Locale.ROOT).startsWith(PREFIX);
    }

    public static String databaseName(String url) {
        return databaseName(url, null);
    }

    /** URL properties take precedence over Properties, matching JDBC semantics. */
    public static String databaseName(String url, Map<String, String> properties) {
        String fromUrl = queryValue(url, "databaseName");
        if (hasText(fromUrl)) {
            return fromUrl.trim();
        }
        String property = propertyIgnoreCase(properties, "databaseName");
        return hasText(property) ? property.trim() : null;
    }

    /**
     * Resolves the effective current schema used for unqualified identifiers.
     * URL properties win, then explicit JDBC properties, then the connector-level schema option.
     */
    public static String currentSchema(
            String url,
            Map<String, String> properties,
            String configuredSchema) {

        String fromUrl = queryValue(url, "currentSchema");
        if (hasText(fromUrl)) {
            return fromUrl.trim();
        }
        String property = propertyIgnoreCase(properties, "currentSchema");
        if (hasText(property)) {
            return property.trim();
        }
        return hasText(configuredSchema) ? configuredSchema.trim() : null;
    }

    static boolean hasCurrentSchemaProperty(
            String url,
            Map<String, String> properties) {
        return hasText(queryValue(url, "currentSchema"))
                || hasText(propertyIgnoreCase(properties, "currentSchema"));
    }

    private static String queryValue(String url, String key) {
        if (!accepts(url)) {
            return null;
        }
        int query = url.indexOf('?');
        if (query < 0 || query == url.length() - 1) {
            return null;
        }
        String queryString = url.substring(query + 1);
        if (queryString.startsWith("/")) {
            queryString = queryString.substring(1);
        }
        for (String item : queryString.split("&")) {
            int equals = item.indexOf('=');
            if (equals <= 0) {
                continue;
            }
            String itemKey = decode(item.substring(0, equals)).trim();
            if (key.equalsIgnoreCase(itemKey)) {
                String value = decode(item.substring(equals + 1)).trim();
                return value.isEmpty() ? null : value;
            }
        }
        return null;
    }

    private static String propertyIgnoreCase(
            Map<String, String> properties,
            String key) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey() != null
                    && key.equalsIgnoreCase(entry.getKey().trim())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
        } catch (Exception ignored) {
            return value;
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
