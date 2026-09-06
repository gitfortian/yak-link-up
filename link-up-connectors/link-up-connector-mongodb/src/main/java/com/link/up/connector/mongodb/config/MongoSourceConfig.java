package com.link.up.connector.mongodb.config;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.mongodb.ConnectionString;
import org.bson.BsonDocument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable bounded MongoDB Source configuration. */
public final class MongoSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String uri;
    private final String database;
    private final String collection;
    private final List<String> fields;
    private final String filter;
    private final int fetchSize;

    private MongoSourceConfig(
            String uri,
            String database,
            String collection,
            List<String> fields,
            String filter,
            int fetchSize) {
        this.uri = uri;
        this.database = database;
        this.collection = collection;
        this.fields = Collections.unmodifiableList(new ArrayList<String>(fields));
        this.filter = filter;
        this.fetchSize = fetchSize;
    }

    public static MongoSourceConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");

        String uri = requireText(config.get(MongoSourceOptions.URI), "uri");
        ConnectionString connectionString = parseConnectionString(uri);
        String configuredDatabase =
                normalize(config.getOptional(MongoSourceOptions.DATABASE).orElse(null));
        String database = configuredDatabase == null
                ? normalize(connectionString.getDatabase())
                : configuredDatabase;
        if (database == null) {
            throw new IllegalArgumentException(
                    "database must be configured when the MongoDB URI has no default database");
        }

        String collection = requireText(
                config.get(MongoSourceOptions.COLLECTION),
                "collection");
        List<String> fields = normalizeFields(
                config.getOptional(MongoSourceOptions.FIELDS)
                        .orElse(Collections.<String>emptyList()));
        String filter = normalize(config.get(MongoSourceOptions.FILTER));
        validateFilter(filter);

        int fetchSize = config.get(MongoSourceOptions.FETCH_SIZE);
        if (fetchSize <= 0) {
            throw new IllegalArgumentException("fetch_size must be greater than 0");
        }

        return new MongoSourceConfig(
                uri,
                database,
                collection,
                fields,
                filter,
                fetchSize);
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

    public List<String> getFields() {
        return fields;
    }

    public String getFilter() {
        return filter;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public boolean hasProjection() {
        return !fields.isEmpty();
    }

    public BsonDocument createFilter() {
        return filter == null ? new BsonDocument() : BsonDocument.parse(filter);
    }

    public TablePath getTablePath() {
        return TablePath.of(database, collection);
    }

    private static ConnectionString parseConnectionString(String uri) {
        try {
            return new ConnectionString(uri);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("Invalid MongoDB connection URI", failure);
        }
    }

    private static List<String> normalizeFields(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }

        Set<String> unique = new LinkedHashSet<String>();
        for (String value : values) {
            String field = normalize(value);
            if (field == null) {
                throw new IllegalArgumentException("fields values must not be blank");
            }
            if (!unique.add(field)) {
                throw new IllegalArgumentException("Duplicate MongoDB field: " + field);
            }
        }
        return new ArrayList<String>(unique);
    }

    private static void validateFilter(String filter) {
        if (filter == null) {
            return;
        }
        try {
            BsonDocument.parse(filter);
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException(
                    "filter must be a valid MongoDB Extended JSON document",
                    failure);
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
