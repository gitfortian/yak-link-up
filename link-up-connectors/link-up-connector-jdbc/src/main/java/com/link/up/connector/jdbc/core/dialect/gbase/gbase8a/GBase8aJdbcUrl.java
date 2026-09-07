package com.link.up.connector.jdbc.core.dialect.gbase.gbase8a;

import java.util.Locale;

/** Dedicated GBase 8a JDBC URL helpers. */
public final class GBase8aJdbcUrl {

    private static final String PREFIX = "jdbc:gbase://";

    private GBase8aJdbcUrl() {
    }

    public static boolean accepts(String url) {
        return url != null
                && url.trim().toLowerCase(Locale.ROOT).startsWith(PREFIX);
    }

    public static String databaseName(String url) {
        if (!accepts(url)) {
            return null;
        }

        String value = url.trim();
        int queryIndex = value.indexOf('?');
        int fragmentIndex = value.indexOf('#');
        int end = value.length();
        if (queryIndex >= 0) {
            end = queryIndex;
        }
        if (fragmentIndex >= 0 && fragmentIndex < end) {
            end = fragmentIndex;
        }

        String main = value.substring(0, end);
        int databaseSeparator = main.indexOf('/', PREFIX.length());
        if (databaseSeparator < 0 || databaseSeparator == main.length() - 1) {
            return null;
        }

        String database = main.substring(databaseSeparator + 1).trim();
        return database.isEmpty() ? null : database;
    }
}
