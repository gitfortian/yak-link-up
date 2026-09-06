package com.link.up.connector.mongodb.sink;

import com.link.up.api.sink.CommitScope;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.mongodb.config.MongoSinkConfig;
import com.link.up.connector.mongodb.converter.MongoFluxRowBsonConverter;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Ordered bounded MongoDB insert writer with task-local durability. */
public final class MongoSinkWriter implements SinkWriter<FluxRow> {

    private final MongoSinkConfig config;
    private final PreparedSinkMetadata metadata;
    private final BatchInsertExecutor insertExecutor;
    private final List<BsonDocument> bufferedDocuments = new ArrayList<BsonDocument>();

    private TableSchema schema;
    private MongoFluxRowBsonConverter converter;
    private long totalWrittenRows;
    private long totalInsertRequests;
    private boolean opened;
    private boolean failed;
    private boolean closed;

    public MongoSinkWriter(
            MongoSinkConfig config,
            PreparedSinkMetadata metadata) {
        this(
                config,
                metadata,
                new MongoClientBatchInsertExecutor(
                        Objects.requireNonNull(config, "config must not be null")));
    }

    MongoSinkWriter(
            MongoSinkConfig config,
            PreparedSinkMetadata metadata,
            BatchInsertExecutor insertExecutor) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
        this.insertExecutor = Objects.requireNonNull(
                insertExecutor,
                "insertExecutor must not be null");
    }

    @Override
    public void open() throws Exception {
        if (opened || closed) {
            throw new IllegalStateException("MongoDB SinkWriter has already been opened or closed");
        }

        boolean success = false;
        try {
            insertExecutor.open();
            opened = true;
            success = true;
        } finally {
            if (!success) {
                try {
                    insertExecutor.close();
                } catch (Exception ignored) {
                    // Preserve the original open failure.
                }
            }
        }
    }

    @Override
    public void write(
            RecordBatch<FluxRow> batch,
            CatalogTable sourceTable)
            throws Exception {
        checkUsable();
        if (batch == null || batch.isEndOfInput() || batch.getRecords().isEmpty()) {
            return;
        }

        initializeSchema(sourceTable);
        for (FluxRow row : batch.getRecords()) {
            bufferedDocuments.add(converter.convert(row));
            if (bufferedDocuments.size() >= config.getBatchSize()) {
                flush();
            }
        }
    }

    @Override
    public void prepareCommit() throws Exception {
        checkUsable();
        flush();
    }

    @Override
    public void commit() {
        checkUsable();
    }

    @Override
    public void abort() {
        bufferedDocuments.clear();
    }

    @Override
    public CommitScope getCommitScope() {
        return CommitScope.TASK_LOCAL;
    }

    @Override
    public String getRetryAdvice() {
        return "MongoDB insertMany makes each successful flush durable before task commit. "
                + "A failed insert request may have inserted a prefix of the ordered batch; "
                + "verify target documents before retrying the whole task.";
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        bufferedDocuments.clear();
        opened = false;
        closed = true;
        insertExecutor.close();
    }

    private void initializeSchema(CatalogTable sourceTable) {
        if (sourceTable == null || sourceTable.getTableSchema() == null) {
            throw new IllegalArgumentException(
                    "MongoDB bounded Sink requires CatalogTable schema on write");
        }
        if (metadata.getTargetTable(sourceTable.getTablePath()) == null) {
            throw new IllegalArgumentException(
                    "MongoDB bounded Sink has no prepared target for source table: "
                            + sourceTable.getTablePath());
        }

        TableSchema incoming = sourceTable.getTableSchema();
        if (schema == null) {
            schema = incoming;
            converter = new MongoFluxRowBsonConverter(
                    schema,
                    config.getDocumentIdField());
            return;
        }
        if (!schema.equals(incoming)) {
            throw new IllegalArgumentException(
                    "MongoDB bounded Sink does not support runtime schema changes");
        }
    }

    private void flush() throws Exception {
        if (bufferedDocuments.isEmpty()) {
            return;
        }
        if (converter == null || schema == null) {
            throw new IllegalStateException(
                    "Cannot flush MongoDB Sink before source schema is initialized");
        }

        List<BsonDocument> request = Collections.unmodifiableList(
                new ArrayList<BsonDocument>(bufferedDocuments));
        try {
            insertExecutor.insert(request);
        } catch (Exception failure) {
            failed = true;
            throw failure;
        }

        totalWrittenRows += request.size();
        totalInsertRequests++;
        bufferedDocuments.clear();
    }

    private void checkUsable() {
        if (!opened || closed) {
            throw new IllegalStateException("MongoDB SinkWriter is not open");
        }
        if (failed) {
            throw new IllegalStateException(
                    "MongoDB SinkWriter cannot continue after an ambiguous insert failure");
        }
    }

    long getTotalWrittenRows() {
        return totalWrittenRows;
    }

    long getTotalInsertRequests() {
        return totalInsertRequests;
    }

    int getBufferedDocuments() {
        return bufferedDocuments.size();
    }

    interface BatchInsertExecutor extends AutoCloseable {

        void open() throws Exception;

        void insert(List<BsonDocument> documents) throws Exception;

        @Override
        void close() throws Exception;
    }

    private static final class MongoClientBatchInsertExecutor
            implements BatchInsertExecutor {

        private final MongoSinkConfig config;

        private MongoClient client;
        private MongoCollection<BsonDocument> collection;

        private MongoClientBatchInsertExecutor(MongoSinkConfig config) {
            this.config = config;
        }

        @Override
        public void open() {
            MongoClient openedClient = MongoClients.create(config.getUri());
            boolean success = false;
            try {
                openedClient.getDatabase(config.getDatabase())
                        .runCommand(new BsonDocument("ping", new BsonInt32(1)));
                MongoCollection<BsonDocument> openedCollection = openedClient
                        .getDatabase(config.getDatabase())
                        .getCollection(config.getCollection(), BsonDocument.class);
                if (!openedCollection.getWriteConcern().isAcknowledged()) {
                    throw new IllegalArgumentException(
                            "MongoDB bounded Sink requires an acknowledged write concern");
                }
                client = openedClient;
                collection = openedCollection;
                success = true;
            } finally {
                if (!success) {
                    openedClient.close();
                }
            }
        }

        @Override
        public void insert(List<BsonDocument> documents) {
            if (collection == null) {
                throw new IllegalStateException("MongoDB batch insert executor is not open");
            }
            collection.insertMany(
                    new ArrayList<BsonDocument>(documents),
                    new InsertManyOptions().ordered(true));
        }

        @Override
        public void close() {
            collection = null;
            if (client != null) {
                client.close();
                client = null;
            }
        }
    }
}
