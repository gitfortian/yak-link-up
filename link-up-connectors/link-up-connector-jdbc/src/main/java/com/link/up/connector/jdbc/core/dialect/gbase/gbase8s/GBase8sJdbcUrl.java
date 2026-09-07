package com.link.up.connector.jdbc.core.dialect.gbase.gbase8s;

import java.util.Locale;
import java.util.Map;

/** GBase 8s JDBC URL helpers. */
public final class GBase8sJdbcUrl {

    public static final String PREFIX = "jdbc:gbasedbt-sqli://";
    public static final String SERVER_PROPERTY = "GBASEDBTSERVER";
    public static final String DELIMIDENT_PROPERTY = "DELIMIDENT";

    private GBase8sJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return hasText(url)
                && url.trim().toLowerCase(Locale.ROOT).startsWith(PREFIX);
    }

    /** Returns the database selected by the JDBC URL, without connection properties. */
    public static String databaseName(String url) {
        if (!accepts(url)) {
            return null;
        }

        String value = url.trim();
        int databaseStart = value.indexOf('/', PREFIX.length());
        if (databaseStart < 0 || databaseStart == value.length() - 1) {
            return null;
        }

        int databaseEnd = value.length();
        int propertiesStart = value.indexOf(':', databaseStart + 1);
        if (propertiesStart >= 0) {
            databaseEnd = propertiesStart;
        }
        int semicolon = value.indexOf(';', databaseStart + 1);
        if (semicolon >= 0 && semicolon < databaseEnd) {
            databaseEnd = semicolon;
        }

        String database = value.substring(databaseStart + 1, databaseEnd).trim();
        return hasText(database) ? database : null;
    }

    /** Resolves GBASEDBTSERVER from explicit properties first, then the URL property segment. */
    public static String serverName(String url, Map<String, String> properties) {
        String configured = property(properties, SERVER_PROPERTY);
        return hasText(configured) ? configured : urlProperty(url, SERVER_PROPERTY);
    }

    /** JDBC defaults DELIMIDENT to n; only an explicit true/y value enables quoted identifiers. */
    public static boolean delimitedIdentifiersEnabled(
            String url,
            Map<String, String> properties) {
        String value = property(properties, DELIMIDENT_PROPERTY);
        if (!hasText(value)) {
            value = urlProperty(url, DELIMIDENT_PROPERTY);
        }
        if (!hasText(value)) {
            return false;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return "y".equals(normalized)
                || "yes".equals(normalized)
                || "true".equals(normalized)
                || "1".equals(normalized);
    }

    private static String urlProperty(String url, String key) {
        if (!accepts(url) || !hasText(key)) {
            return null;
        }
        String value = url.trim();
        int databaseStart = value.indexOf('/', PREFIX.length());
        if (databaseStart < 0) {
            return null;
        }
        int propertiesStart = value.indexOf(':', databaseStart + 1);
        if (propertiesStart < 0 || propertiesStart == value.length() - 1) {
            return null;
        }

        String[] pairs = value.substring(propertiesStart + 1).split(";");
        for (String pair : pairs) {
            int equals = pair.indexOf('=');
            if (equals <= 0) {
                continue;
            }
            String propertyKey = pair.substring(0, equals).trim();
            if (key.equalsIgnoreCase(propertyKey)) {
                String propertyValue = pair.substring(equals + 1).trim();
                return hasText(propertyValue) ? propertyValue : null;
            }
        }
        return null;
    }

    private static String property(Map<String, String> properties, String key) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (key.equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
