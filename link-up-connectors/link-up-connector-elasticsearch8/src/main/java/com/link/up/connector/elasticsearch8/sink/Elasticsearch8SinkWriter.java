package com.link.up.connector.elasticsearch8.sink;

import com.link.up.api.sink.CommitScope;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.elasticsearch8.client.Elasticsearch8SinkClient;
import com.link.up.connector.elasticsearch8.config.Elasticsearch8SinkConfig;
import com.link.up.connector.elasticsearch8.converter.Elasticsearch8DocumentConverter;
import com.link.up.connector.elasticsearch8.converter.Elasticsearch8DocumentConverter.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Bounded Elasticsearch 8 writer using synchronous Java API Client Bulk requests. */
public final class Elasticsearch8SinkWriter implements SinkWriter<FluxRow> {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8SinkWriter.class);

    private final Elasticsearch8SinkConfig config;
    private final PreparedSinkMetadata metadata;
    private final BulkExecutor bulkExecutor;
    private final List<Document> pending = new ArrayList<Document>();

    private TableSchema sourceSchema;
    private Elasticsearch8DocumentConverter converter;
    private long totalWrittenRows;
    private long totalBulkRequests;
    private boolean executorOpened;
    private boolean opened;
    private boolean failed;

    public Elasticsearch8SinkWriter(
            Elasticsearch8SinkConfig config,
            PreparedSinkMetadata metadata) {
        this(config, metadata, new ClientBulkExecutor(config));
    }

    Elasticsearch8SinkWriter(
            Elasticsearch8SinkConfig config,
            PreparedSinkMetadata metadata,
            BulkExecutor bulkExecutor) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
        this.bulkExecutor = Objects.requireNonNull(
                bulkExecutor, "bulkExecutor must not be null");
    }

    @Override
    public void open() {
        if (opened) {
            throw new IllegalStateException(
                    "Elasticsearch8SinkWriter has already been opened");
        }
        opened = true;
        LOG.info(
                "Elasticsearch 8 SinkWriter opened: target={}, batchSize={}, documentIdField={}",
                config.getIndex(),
                config.getBatchSize(),
                config.getDocumentIdField());
    }

    @Override
    public void write(
            RecordBatch<FluxRow> batch,
            CatalogTable sourceTable)
            throws Exception {
        checkWritable();
        if (batch == null || batch.isEndOfInput() || batch.getRecords().isEmpty()) {
            return;
        }
        initializeSchema(sourceTable);

        for (FluxRow row : batch.getRecords()) {
            pending.add(converter.convert(row));
            if (pending.size() >= config.getBatchSize()) {
                flush();
            }
        }
    }

    @Override
    public void prepareCommit() throws Exception {
        checkWritable();
        flush();
    }

    @Override
    public void commit() {
        checkOpened();
        LOG.info(
                "Elasticsearch 8 SinkWriter commit boundary reached: totalWrittenRows={}, totalBulkRequests={}",
                totalWrittenRows,
                totalBulkRequests);
    }

    @Override
    public void abort() {
        pending.clear();
        LOG.warn(
                "Elasticsearch 8 SinkWriter aborted unsent documents; successful Bulk requests cannot be rolled back: writtenRows={}",
                totalWrittenRows);
    }

    @Override
    public CommitScope getCommitScope() {
        return CommitScope.TASK_LOCAL;
    }

    @Override
    public String getRetryAdvice() {
        if (config.getDocumentIdField() == null) {
            return "Elasticsearch Bulk requests are durable as they succeed. Whole-task retry may create duplicate documents because document_id_field is not configured. Whole-request Bulk failures are not retried automatically because server-side success is ambiguous.";
        }
        return "Elasticsearch Bulk requests are durable as they succeed. document_id_field provides stable _id values for deterministic re-indexing, but this Sink still does not provide job-level atomicity. Whole-request Bulk failures are not retried automatically because server-side success is ambiguous.";
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        try {
            if (executorOpened) {
                bulkExecutor.close();
            }
        } catch (Exception closeFailure) {
            failure = closeFailure;
        } finally {
            pending.clear();
            converter = null;
            sourceSchema = null;
            executorOpened = false;
            opened = false;
        }
        LOG.info(
                "Elasticsearch 8 SinkWriter closed without implicit flush: totalWrittenRows={}, totalBulkRequests={}",
                totalWrittenRows,
                totalBulkRequests);
        if (failure != null) {
            throw failure;
        }
    }

    private void initializeSchema(CatalogTable sourceTable) throws Exception {
        if (sourceTable == null || sourceTable.getTableSchema() == null) {
            throw new IllegalArgumentException(
                    "Elasticsearch 8 Sink requires CatalogTable schema on write");
        }
        TableSchema incoming = sourceTable.getTableSchema();
        if (sourceSchema != null) {
            if (!sourceSchema.equals(incoming)) {
                throw new IllegalArgumentException(
                        "Elasticsearch 8 bounded Sink does not support runtime schema changes");
            }
            return;
        }

        CatalogTable targetTable = metadata.getTargetTable(sourceTable.getTablePath());
        if (targetTable == null) {
            throw new IllegalArgumentException(
                    "Prepared Elasticsearch 8 target metadata is missing source table mapping: "
                            + sourceTable.getTablePath());
        }
        sourceSchema = incoming;
        converter = new Elasticsearch8DocumentConverter(
                sourceTable,
                targetTable,
                config.getDocumentIdField());
        bulkExecutor.open();
        executorOpened = true;
    }

    private void flush() throws Exception {
        if (pending.isEmpty()) {
            return;
        }
        if (!executorOpened) {
            throw new IllegalStateException(
                    "Cannot flush Elasticsearch 8 Sink before schema initialization");
        }

        List<Document> batch =
                Collections.unmodifiableList(new ArrayList<Document>(pending));
        try {
            int written = bulkExecutor.execute(batch);
            if (written != batch.size()) {
                throw new IllegalStateException(
                        "Elasticsearch Bulk executor wrote an unexpected document count: expected="
                                + batch.size() + ", actual=" + written);
            }
            pending.clear();
            totalWrittenRows += written;
            totalBulkRequests++;
            LOG.debug(
                    "Flushed Elasticsearch 8 Bulk request: rows={}, totalWrittenRows={}",
                    written,
                    totalWrittenRows);
        } catch (Exception failure) {
            failed = true;
            throw failure;
        }
    }

    private void checkWritable() {
        checkOpened();
        if (failed) {
            throw new IllegalStateException(
                    "Elasticsearch8SinkWriter is in failed state after a previous Bulk failure");
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException(
                    "Elasticsearch8SinkWriter has not been opened");
        }
    }

    interface BulkExecutor extends AutoCloseable {
        void open() throws Exception;
        int execute(List<Document> documents) throws Exception;
        @Override
        void close() throws Exception;
    }

    private static final class ClientBulkExecutor implements BulkExecutor {
        private final Elasticsearch8SinkConfig config;
        private Elasticsearch8SinkClient client;

        private ClientBulkExecutor(Elasticsearch8SinkConfig config) {
            this.config = config;
        }

        @Override
        public void open() throws Exception {
            client = new Elasticsearch8SinkClient(config);
            boolean success = false;
            try {
                client.verifyMajorVersion();
                success = true;
            } finally {
                if (!success) {
                    client.close();
                    client = null;
                }
            }
        }

        @Override
        public int execute(List<Document> documents) throws Exception {
            return client.bulkIndex(documents);
        }

        @Override
        public void close() throws Exception {
            if (client != null) {
                client.close();
                client = null;
            }
        }
    }
}
