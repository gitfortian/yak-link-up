package com.link.up.connector.clickhouse.sink;

import com.link.up.api.sink.CommitScope;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.clickhouse.client.ClickHouseSinkJdbcClient;
import com.link.up.connector.clickhouse.config.ClickHouseSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/** Bounded ClickHouse writer using one synchronous JDBC prepared batch at a time. */
public final class ClickHouseSinkWriter implements SinkWriter<FluxRow> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseSinkWriter.class);

    private final ClickHouseSinkConfig config;
    private final PreparedSinkMetadata metadata;
    private final BatchExecutor batchExecutor;

    private TableSchema sourceSchema;
    private int pendingRows;
    private long totalWrittenRows;
    private long totalBatchRequests;
    private boolean executorOpened;
    private boolean opened;

    public ClickHouseSinkWriter(
            ClickHouseSinkConfig config,
            PreparedSinkMetadata metadata) {
        this(config, metadata, new JdbcBatchExecutor(config));
    }

    ClickHouseSinkWriter(
            ClickHouseSinkConfig config,
            PreparedSinkMetadata metadata,
            BatchExecutor batchExecutor) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
        this.batchExecutor = Objects.requireNonNull(batchExecutor, "batchExecutor must not be null");
    }

    @Override
    public void open() {
        if (opened) {
            throw new IllegalStateException("ClickHouseSinkWriter has already been opened");
        }
        opened = true;
        LOG.info(
                "ClickHouse SinkWriter opened: target={}.{}, batchSize={}, host={}",
                config.getDatabase(),
                config.getTable(),
                config.getBatchSize(),
                config.getHost());
    }

    @Override
    public void write(
            RecordBatch<FluxRow> batch,
            CatalogTable sourceTable)
            throws Exception {
        checkOpened();
        if (batch == null || batch.isEndOfInput() || batch.getRecords().isEmpty()) {
            return;
        }
        initializeSchema(sourceTable);

        for (FluxRow row : batch.getRecords()) {
            batchExecutor.add(row);
            pendingRows++;
            if (pendingRows >= config.getBatchSize()) {
                flush();
            }
        }
    }

    @Override
    public void prepareCommit() throws Exception {
        checkOpened();
        flush();
    }

    @Override
    public void commit() {
        checkOpened();
        LOG.info(
                "ClickHouse SinkWriter commit boundary reached: totalWrittenRows={}, totalBatchRequests={}",
                totalWrittenRows,
                totalBatchRequests);
    }

    @Override
    public void abort() throws Exception {
        if (executorOpened) {
            batchExecutor.clearBatch();
        }
        pendingRows = 0;
        LOG.warn(
                "ClickHouse SinkWriter aborted unsent batch; already successful executeBatch calls cannot be rolled back: writtenRows={}",
                totalWrittenRows);
    }

    @Override
    public CommitScope getCommitScope() {
        return CommitScope.TASK_LOCAL;
    }

    @Override
    public String getRetryAdvice() {
        return "ClickHouse commits every successful JDBC executeBatch independently. "
                + "Ambiguous executeBatch failures are not retried automatically; verify already inserted target data before retrying a whole task.";
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        try {
            if (executorOpened) {
                batchExecutor.close();
            }
        } catch (Exception closeFailure) {
            failure = closeFailure;
        } finally {
            opened = false;
            executorOpened = false;
            pendingRows = 0;
        }
        LOG.info(
                "ClickHouse SinkWriter closed without implicit flush: totalWrittenRows={}, totalBatchRequests={}",
                totalWrittenRows,
                totalBatchRequests);
        if (failure != null) {
            throw failure;
        }
    }

    private void initializeSchema(CatalogTable sourceTable) throws Exception {
        if (sourceTable == null || sourceTable.getTableSchema() == null) {
            throw new IllegalArgumentException("ClickHouse Sink requires CatalogTable schema on write");
        }
        TableSchema incoming = sourceTable.getTableSchema();
        if (sourceSchema != null) {
            if (!sourceSchema.equals(incoming)) {
                throw new IllegalArgumentException(
                        "ClickHouse bounded Sink does not support runtime schema changes");
            }
            return;
        }

        CatalogTable targetTable = metadata.getTargetTable(sourceTable.getTablePath());
        if (targetTable == null) {
            throw new IllegalArgumentException(
                    "Prepared ClickHouse target metadata is missing source table mapping: "
                            + sourceTable.getTablePath());
        }
        sourceSchema = incoming;
        batchExecutor.open(sourceTable, targetTable);
        executorOpened = true;
    }

    private void flush() throws Exception {
        if (pendingRows == 0) {
            return;
        }
        if (!executorOpened) {
            throw new IllegalStateException("Cannot flush ClickHouse Sink before schema initialization");
        }
        int expectedRows = pendingRows;
        int flushedRows = batchExecutor.flush();
        if (flushedRows != expectedRows) {
            throw new IllegalStateException(
                    "ClickHouse batch executor flushed an unexpected row count: expected="
                            + expectedRows
                            + ", actual="
                            + flushedRows);
        }
        totalWrittenRows += flushedRows;
        totalBatchRequests++;
        pendingRows = 0;
        LOG.debug(
                "Flushed ClickHouse JDBC batch: rows={}, totalWrittenRows={}",
                flushedRows,
                totalWrittenRows);
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("ClickHouseSinkWriter has not been opened");
        }
    }

    interface BatchExecutor extends AutoCloseable {
        void open(CatalogTable sourceTable, CatalogTable targetTable) throws Exception;

        void add(FluxRow row) throws Exception;

        int flush() throws Exception;

        void clearBatch() throws Exception;

        @Override
        void close() throws Exception;
    }

    private static final class JdbcBatchExecutor implements BatchExecutor {
        private final ClickHouseSinkConfig config;
        private ClickHouseSinkJdbcClient client;

        private JdbcBatchExecutor(ClickHouseSinkConfig config) {
            this.config = config;
        }

        @Override
        public void open(CatalogTable sourceTable, CatalogTable targetTable) throws Exception {
            client =
                    new ClickHouseSinkJdbcClient(
                            config,
                            targetTable,
                            sourceTable.getTableSchema());
            client.open();
        }

        @Override
        public void add(FluxRow row) throws Exception {
            client.add(row);
        }

        @Override
        public int flush() throws Exception {
            return client.flush();
        }

        @Override
        public void clearBatch() throws Exception {
            if (client != null) {
                client.clearBatch();
            }
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
