package com.link.up.connector.mongodb.config;

import com.mongodb.ConnectionString;

import java.io.Serializable;
import java.util.Objects;

/**
 * MongoDB Catalog configuration.
 *
 * <p>Stage 1 intentionally keeps schema controls internal to the connector. Users only need a
 * MongoDB URI; field types are discovered from collection documents.
 */
public final class MongoCatalogConfig implements Serializable {

    public static final int DEFAULT_SCHEMA_SAMPLE_SIZE = 1000;
    public static final int DEFAULT_SCHEMA_MAX_DEPTH = 8;

    private static final long serialVersionUID = 1L;
    private static final int MAX_SCHEMA_MAX_DEPTH = 32;

    private final String uri;
    private final String defaultDatabase;
    private final int schemaSampleSize;
    private final int schemaMaxDepth;

    public MongoCatalogConfig(String uri, int schemaSampleSize, int schemaMaxDepth) {
        this.uri = requireText(uri, "uri");
        this.defaultDatabase = parseDefaultDatabase(this.uri);

        if (schemaSampleSize <= 0) {
            throw new IllegalArgumentException("schemaSampleSize must be greater than 0");
        }
        if (schemaMaxDepth < 0 || schemaMaxDepth > MAX_SCHEMA_MAX_DEPTH) {
            throw new IllegalArgumentException(
                    "schemaMaxDepth must be between 0 and " + MAX_SCHEMA_MAX_DEPTH);
        }

        this.schemaSampleSize = schemaSampleSize;
        this.schemaMaxDepth = schemaMaxDepth;
    }

    public static MongoCatalogConfig of(String uri) {
        return new MongoCatalogConfig(
                uri,
                DEFAULT_SCHEMA_SAMPLE_SIZE,
                DEFAULT_SCHEMA_MAX_DEPTH);
    }

    public String getUri() {
        return uri;
    }

    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public int getSchemaSampleSize() {
        return schemaSampleSize;
    }

    public int getSchemaMaxDepth() {
        return schemaMaxDepth;
    }

    private static String parseDefaultDatabase(String uri) {
        try {
            return normalize(new ConnectionString(uri).getDatabase());
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("Invalid MongoDB connection URI", failure);
        }
    }

    private static String requireText(String value, String fieldName) {
        String normalized = normalize(value);
        if (normalized == null) {
            throw new IllegalArgumentException(fieldName + " must not be empty");
        }
        return normalized;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MongoCatalogConfig)) {
            return false;
        }
        MongoCatalogConfig that = (MongoCatalogConfig) obj;
        return schemaSampleSize == that.schemaSampleSize
                && schemaMaxDepth == that.schemaMaxDepth
                && Objects.equals(uri, that.uri)
                && Objects.equals(defaultDatabase, that.defaultDatabase);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, defaultDatabase, schemaSampleSize, schemaMaxDepth);
    }
}
