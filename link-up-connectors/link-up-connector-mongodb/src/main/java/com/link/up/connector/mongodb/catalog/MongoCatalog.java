package com.link.up.connector.mongodb.catalog;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.catalog.exception.TableNotFoundException;
import com.link.up.connector.mongodb.config.MongoCatalogConfig;
import com.link.up.connector.mongodb.schema.MongoSchemaInference;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * MongoDB metadata catalog.
 *
 * <p>MongoDB has no enforced table schema, so {@link #getTable(TablePath)} samples collection
 * documents and synthesizes a stable Link-Up {@link TableSchema}. The BSON details remain inside
 * column metadata and are not exposed as global Link-Up types.
 */
public final class MongoCatalog implements Catalog {

    public static final String IDENTIFIER = "mongodb";

    private final String catalogName;
    private final MongoCatalogConfig config;

    private volatile MongoClient client;

    public MongoCatalog(MongoCatalogConfig config) {
        this(IDENTIFIER, config);
    }

    public MongoCatalog(String catalogName, MongoCatalogConfig config) {
        this.catalogName = requireText(catalogName, "catalogName");
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public Optional<String> getDefaultDatabase() {
        return Optional.ofNullable(config.getDefaultDatabase());
    }

    @Override
    public synchronized void open() throws CatalogException {
        if (client != null) {
            return;
        }

        MongoClient openedClient = null;
        try {
            openedClient = MongoClients.create(config.getUri());
            String pingDatabase = config.getDefaultDatabase() == null
                    ? "admin"
                    : config.getDefaultDatabase();
            openedClient.getDatabase(pingDatabase)
                    .runCommand(new BsonDocument("ping", new BsonInt32(1)));
            client = openedClient;
        } catch (RuntimeException failure) {
            if (openedClient != null) {
                openedClient.close();
            }
            throw new CatalogException(
                    "MongoDB Catalog connection failed, catalog=" + catalogName,
                    failure);
        }
    }

    @Override
    public synchronized void close() {
        MongoClient current = client;
        client = null;
        if (current != null) {
            current.close();
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            List<String> databases = new ArrayList<String>();
            for (String databaseName : currentClient().listDatabaseNames()) {
                databases.add(databaseName);
            }
            Collections.sort(databases);
            return databases;
        } catch (MongoException failure) {
            throw new CatalogException(
                    "Could not list MongoDB databases, catalog=" + catalogName,
                    failure);
        }
    }

    @Override
    public List<TablePath> listTables(String databaseName, String schemaName)
            throws CatalogException {
        rejectSchema(schemaName);
        String resolvedDatabase = resolveDatabase(databaseName);

        try {
            List<String> collectionNames = new ArrayList<String>();
            for (String collectionName : currentClient()
                    .getDatabase(resolvedDatabase)
                    .listCollectionNames()) {
                collectionNames.add(collectionName);
            }
            Collections.sort(collectionNames);

            List<TablePath> tables = new ArrayList<TablePath>(collectionNames.size());
            for (String collectionName : collectionNames) {
                tables.add(TablePath.of(resolvedDatabase, collectionName));
            }
            return tables;
        } catch (MongoException failure) {
            throw new CatalogException(
                    "Could not list MongoDB collections, database=" + resolvedDatabase,
                    failure);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        TablePath resolvedPath = resolveTablePath(tablePath);
        try {
            MongoDatabase database = currentClient().getDatabase(resolvedPath.getDatabaseName());
            for (String collectionName : database.listCollectionNames()) {
                if (resolvedPath.getTableName().equals(collectionName)) {
                    return true;
                }
            }
            return false;
        } catch (MongoException failure) {
            throw new CatalogException(
                    "Could not check MongoDB collection, table=" + resolvedPath,
                    failure);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotFoundException {
        TablePath resolvedPath = resolveTablePath(tablePath);
        if (!tableExists(resolvedPath)) {
            throw new TableNotFoundException(catalogName, resolvedPath);
        }

        try {
            MongoCollection<BsonDocument> collection = currentClient()
                    .getDatabase(resolvedPath.getDatabaseName())
                    .getCollection(resolvedPath.getTableName(), BsonDocument.class);

            MongoSchemaInference inference = new MongoSchemaInference(config.getSchemaMaxDepth());
            try (MongoCursor<BsonDocument> cursor = collection.find()
                    .limit(config.getSchemaSampleSize())
                    .iterator()) {
                while (cursor.hasNext()) {
                    inference.observe(cursor.next());
                }
            }

            TableSchema schema = inference.build();
            String discoveryMode = inference.getSampledDocuments() == 0
                    ? "synthetic-empty"
                    : "sampled";

            return CatalogTable.builder(resolvedPath, schema)
                    .option("mongodb.collection", resolvedPath.getTableName())
                    .option("mongodb.schema.discovery", discoveryMode)
                    .option("mongodb.schema.sampleSize", String.valueOf(config.getSchemaSampleSize()))
                    .option("mongodb.schema.sampledDocuments", String.valueOf(inference.getSampledDocuments()))
                    .option("mongodb.schema.maxDepth", String.valueOf(config.getSchemaMaxDepth()))
                    .build();
        } catch (MongoException failure) {
            throw new CatalogException(
                    "Could not discover MongoDB collection schema, table=" + resolvedPath,
                    failure);
        }
    }

    private MongoClient currentClient() throws CatalogException {
        MongoClient current = client;
        if (current == null) {
            throw new CatalogException("MongoDB Catalog is not open, catalog=" + catalogName);
        }
        return current;
    }

    private TablePath resolveTablePath(TablePath tablePath) throws CatalogException {
        Objects.requireNonNull(tablePath, "tablePath must not be null");
        rejectSchema(tablePath.getSchemaName());
        return TablePath.of(
                resolveDatabase(tablePath.getDatabaseName()),
                tablePath.getTableName());
    }

    private String resolveDatabase(String databaseName) throws CatalogException {
        String normalized = normalize(databaseName);
        if (normalized != null) {
            return normalized;
        }
        if (config.getDefaultDatabase() != null) {
            return config.getDefaultDatabase();
        }
        throw new CatalogException(
                "MongoDB database must be specified when the connection URI has no default database");
    }

    private static void rejectSchema(String schemaName) throws CatalogException {
        if (normalize(schemaName) != null) {
            throw new CatalogException("MongoDB does not have an independent schema namespace");
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
