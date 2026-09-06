package com.link.up.connector.mongodb.config;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.mongodb.ConnectionString;

import java.io.Serializable;
import java.util.Objects;

/** Immutable bounded MongoDB Sink configuration. */
public final class MongoSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String uri;
    private final String database;
    private final String collection;
    private final String documentIdField;
    private final int batchSize;

    private MongoSinkConfig(
            String uri,
            String database,
            String collection,
            String documentIdField,
            int batchSize) {
        this.uri = uri;
        this.database = database;
        this.collection = collection;
        this.documentIdField = documentIdField;
        this.batchSize = batchSize;
    }

    public static MongoSinkConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");

        String uri = requireText(config.get(MongoSinkOptions.URI), "uri");
        ConnectionString connectionString = parseConnectionString(uri);
        String configuredDatabase =
                normalize(config.getOptional(MongoSinkOptions.DATABASE).orElse(null));
        String database = configuredDatabase == null
                ? normalize(connectionString.getDatabase())
                : configuredDatabase;
        if (database == null) {
            throw new IllegalArgumentException(
                    "database must be configured when the MongoDB URI has no default database");
        }

        String collection = requireText(
                config.get(MongoSinkOptions.COLLECTION),
                "collection");
        String documentIdField = normalize(
                config.getOptional(MongoSinkOptions.DOCUMENT_ID_FIELD).orElse(null));
        int batchSize = config.get(MongoSinkOptions.BATCH_SIZE);
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batch_size must be greater than 0");
        }

        return new MongoSinkConfig(
                uri,
                database,
                collection,
                documentIdField,
                batchSize);
    }

    public String getUri() {
        return uri;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public String getDocumentIdField() {
        return documentIdField;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public TablePath getTargetPath() {
        return TablePath.of(database, collection);
    }

    private static ConnectionString parseConnectionString(String uri) {
        try {
            return new ConnectionString(uri);
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
}
