package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import java.util.Locale;

/**
 * Stable GBase 8c database compatibility modes exposed by pg_database.datcompatibility.
 *
 * <p>The database chooses this mode when it is created. Automatic target DDL must resolve and
 * honor the actual target database mode instead of assuming PostgreSQL semantics.</p>
 */
public enum GBase8cCompatibilityMode {

    /** Oracle-compatible database. */
    A("A", "oracle"),

    /** MySQL-compatible database. */
    B("B", "mysql"),

    /** Teradata-compatible database. */
    C("C", "teradata"),

    /** PostgreSQL-compatible database. */
    PG("PG", "postgresql");

    private final String databaseValue;
    private final String compatibleProduct;

    GBase8cCompatibilityMode(
            String databaseValue,
            String compatibleProduct) {
        this.databaseValue = databaseValue;
        this.compatibleProduct = compatibleProduct;
    }

    public String databaseValue() {
        return databaseValue;
    }

    public String compatibleProduct() {
        return compatibleProduct;
    }

    /**
     * Parses the exact value returned by {@code pg_database.datcompatibility}.
     *
     * <p>Unknown/newer modes are rejected deliberately. Existing Source/Sink execution does not
     * depend on this parser; only compatibility-sensitive DDL code should invoke it.</p>
     */
    public static GBase8cCompatibilityMode fromDatabaseValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "GBase 8c datcompatibility must not be empty");
        }
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        for (GBase8cCompatibilityMode mode : values()) {
            if (mode.databaseValue.equals(normalized)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                "Unsupported GBase 8c datcompatibility='"
                        + value
                        + "'; safe automatic DDL currently recognizes A, B, C and PG only");
    }
}
