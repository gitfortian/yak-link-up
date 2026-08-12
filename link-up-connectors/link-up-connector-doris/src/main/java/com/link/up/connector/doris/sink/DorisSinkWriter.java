package com.link.up.connector.doris.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.sink.CommitScope;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.doris.client.DorisStreamLoadClient;
import com.link.up.connector.doris.client.DorisStreamLoadClient.StreamLoadResponse;
import com.link.up.connector.doris.config.DorisSinkConfig;
import com.link.up.connector.doris.serializer.DorisRowSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Doris SinkWriter。
 *
 * <p>通过 Stream Load 将数据批量写入 Doris。
 * 内部维护一个行缓冲区，达到 {@code doris.batch.size} 阈值时
 * 触发一次 Stream Load HTTP 请求。
 *
 * <p>支持两种事务模式：
 * <ul>
 *   <li><b>非 2PC 模式</b>（默认）：每次 flush 立即提交，at-least-once 语义。</li>
 *   <li><b>2PC 模式</b>（{@code sink.enable-2pc=true}）：每次 flush 产生一个 PREPARE 事务，
 *       所有数据写入完成后在 {@link #commit()} 中统一提交，实现 exactly-once 语义。</li>
 * </ul>
 */
public final class DorisSinkWriter implements SinkWriter<FluxRow> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DorisSinkWriter.class);

    private final DorisSinkConfig config;
    private final PreparedSinkMetadata metadata;
    private final DorisStreamLoadClient client;
    private final List<FluxRow> buffer = new ArrayList<>();
    private final boolean enable2pc;
    private TableSchema schema;

    private final List<String> pendingTxnIds = new ArrayList<>();

    private long totalWrittenRows = 0;
    private long totalLoadRequests = 0;

    public DorisSinkWriter(DorisSinkConfig config, PreparedSinkMetadata metadata) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
        this.client = new DorisStreamLoadClient(config);
        this.enable2pc = config.isEnable2pc();
    }

    @Override
    public void open() throws Exception {
        LOG.info("Doris SinkWriter opened: database={}, table={}, batchSize={}, format={}, 2pc={}",
                config.getDatabase(), config.getTable(),
                config.getBatchSize(), config.getLoadFormat(), enable2pc);
    }

    @Override
    public void write(RecordBatch<FluxRow> batch, CatalogTable sourceTable) throws Exception {
        if (batch == null || batch.isEndOfInput() || batch.getRecords().isEmpty()) {
            return;
        }

        if (schema == null && sourceTable != null && sourceTable.getTableSchema() != null) {
            schema = sourceTable.getTableSchema();
        }

        for (FluxRow row : batch.getRecords()) {
            buffer.add(row);

            if (buffer.size() >= config.getBatchSize()) {
                flush();
            }
        }
    }

    @Override
    public void prepareCommit() throws Exception {
        flush();
    }

    @Override
    public void commit() throws Exception {
        if (enable2pc && !pendingTxnIds.isEmpty()) {
            LOG.info("Committing {} pending 2PC transactions...", pendingTxnIds.size());
            try {
                client.commitTransactions(pendingTxnIds);
                LOG.info("All {} 2PC transactions committed successfully", pendingTxnIds.size());
            } catch (IOException e) {
                LOG.error("2PC commit failed, {} transactions may be in inconsistent state. "
                        + "Committed transactions will not be rolled back.",
                        pendingTxnIds.size(), e);
                throw new IOException("Doris 2PC commit failed with " + pendingTxnIds.size()
                        + " pending transactions. Some may have been committed. "
                        + "Check Doris transaction state before retrying.", e);
            }
            pendingTxnIds.clear();
        }
        LOG.info("Doris SinkWriter commit: totalWrittenRows={}, totalLoadRequests={}, 2pc={}",
                totalWrittenRows, totalLoadRequests, enable2pc);
    }

    @Override
    public void abort() throws Exception {
        buffer.clear();
        if (enable2pc && !pendingTxnIds.isEmpty()) {
            LOG.warn("Aborting {} pending 2PC transactions...", pendingTxnIds.size());
            client.abortTransactions(pendingTxnIds);
            LOG.info("All {} 2PC transactions aborted", pendingTxnIds.size());
            pendingTxnIds.clear();
        }
        LOG.warn("Doris SinkWriter aborted, buffer cleared. totalWrittenRows={}", totalWrittenRows);
    }

    @Override
    public CommitScope getCommitScope() {
        return CommitScope.TASK_LOCAL;
    }

    @Override
    public String getRetryAdvice() {
        if (enable2pc) {
            return "Doris 2PC mode: data is PREPARED but not committed. "
                    + "Safe to retry — uncommitted transactions will be aborted.";
        }
        return "Doris Stream Load commits per batch; verify already loaded data before retrying.";
    }

    @Override
    public void close() throws Exception {
        try {
            if (enable2pc && !pendingTxnIds.isEmpty()) {
                LOG.warn("Closing with {} uncommitted 2PC transactions, aborting...", pendingTxnIds.size());
                client.abortTransactions(pendingTxnIds);
                pendingTxnIds.clear();
            }
            if (!enable2pc && !buffer.isEmpty()) {
                flush();
            }
        } finally {
            client.close();
        }
        LOG.info("Doris SinkWriter closed: totalWrittenRows={}, totalLoadRequests={}, 2pc={}",
                totalWrittenRows, totalLoadRequests, enable2pc);
    }

    private void flush() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }

        if (schema == null) {
            throw new IllegalStateException(
                    "Cannot flush: table schema is not initialized. "
                            + "Ensure at least one write() call provides a CatalogTable with schema.");
        }

        DorisRowSerializer serializer = new DorisRowSerializer(config, schema);
        String data = serializer.serialize(buffer);

        LOG.debug("Flushing {} rows via Stream Load, data size={} bytes",
                buffer.size(), data.length());

        StreamLoadResponse response = client.load(data);
        response.checkSuccess();

        totalWrittenRows += buffer.size();
        totalLoadRequests++;

        LOG.debug("Stream Load success: label={}, loadedRows={}, txnId={}, txnState={}",
                response.getLabel(), response.getNumberLoadedRows(),
                response.getTxnId(), response.getTxnState());

        if (response.getNumberFilteredRows() > 0) {
            LOG.warn("Stream Load filtered {} rows, message: {}",
                    response.getNumberFilteredRows(), response.getMessage());
        }

        if (enable2pc) {
            String txnId = response.getTxnId();
            if (txnId == null || txnId.isEmpty()) {
                throw new IOException(
                        "2PC enabled but Doris returned no TxnId. "
                        + "Response: " + response.getBody());
            }
            if (!response.isPrepared()) {
                throw new IOException(
                        "2PC enabled but TxnState is not PREPARE: txnId=" + txnId
                        + ", txnState=" + response.getTxnState()
                        + ", response=" + response.getBody());
            }
            pendingTxnIds.add(txnId);
            LOG.debug("Collected 2PC txnId={}, pending count={}", txnId, pendingTxnIds.size());
        }

        buffer.clear();
    }
}
