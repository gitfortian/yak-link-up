package com.link.up.connector.doris.client.source;

import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.FluxRowType;
import com.link.up.connector.doris.client.source.model.DorisQueryPartition;
import com.link.up.connector.doris.config.DorisSourceConfig;
import com.link.up.connector.doris.config.DorisSourceTableConfig;
import com.link.up.connector.doris.converter.DorisArrowRowReader;
import org.apache.doris.sdk.thrift.TDorisExternalService;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanCloseResult;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.apache.doris.sdk.thrift.TStatus;
import org.apache.doris.sdk.thrift.TStatusCode;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Task-local Thrift client for the Doris BE external scanner service. */
public final class DorisBeReadClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisBeReadClient.class);
    private static final String DEFAULT_CLUSTER_NAME = "default_cluster";

    private final DorisSourceConfig config;
    private final DorisSourceTableConfig tableConfig;
    private final DorisQueryPartition partition;
    private final FluxRowType rowType;

    private TSocket socket;
    private TDorisExternalService.Client client;
    private String contextId;
    private int readerOffset;
    private boolean eos;
    private List<FluxRow> currentRows = Collections.emptyList();
    private int currentRowIndex;

    public DorisBeReadClient(
            DorisSourceConfig config,
            DorisSourceTableConfig tableConfig,
            DorisQueryPartition partition,
            FluxRowType rowType) {
        this.config = config;
        this.tableConfig = tableConfig;
        this.partition = partition;
        this.rowType = rowType;
    }

    public void open() throws Exception {
        Exception lastFailure = null;
        int attempts = Math.max(1, config.getRequestRetries() + 1);
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                openConnection();
                TScanOpenResult result = client.openScanner(openParams());
                ensureOk("openScanner", result == null ? null : result.getStatus());
                if (result == null
                        || result.getContextId() == null
                        || result.getContextId().trim().isEmpty()) {
                    throw new IOException("Doris BE openScanner returned an empty context id");
                }
                contextId = result.getContextId();
                readerOffset = 0;
                eos = false;
                currentRows = Collections.emptyList();
                currentRowIndex = 0;
                LOG.info(
                        "Opened Doris native scanner: table={}.{}, be={}, tablets={}, contextId={}",
                        partition.getDatabase(),
                        partition.getTable(),
                        partition.getBeAddress(),
                        partition.getTabletIds().size(),
                        contextId);
                return;
            } catch (Exception failure) {
                lastFailure = failure;
                closeTransport();
                LOG.warn(
                        "Doris BE scanner open failed: be={}, attempt={}/{}, error={}",
                        partition.getBeAddress(),
                        attempt + 1,
                        attempts,
                        failure.getMessage());
            }
        }
        throw new IOException(
                "Unable to open Doris BE scanner for " + partition.getBeAddress(), lastFailure);
    }

    public boolean hasNext() throws Exception {
        while (currentRowIndex >= currentRows.size() && !eos) {
            fetchNextBatch();
        }
        return currentRowIndex < currentRows.size();
    }

    public FluxRow next() throws Exception {
        if (!hasNext()) {
            throw new IllegalStateException("No more Doris rows in current split");
        }
        return currentRows.get(currentRowIndex++);
    }

    private void openConnection() throws Exception {
        String[] hostPort = parseBeAddress(partition.getBeAddress());
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);
        socket =
                new TSocket(
                        new TConfiguration(),
                        host,
                        port,
                        config.getReadTimeoutMs(),
                        config.getConnectTimeoutMs());
        socket.open();
        TProtocol protocol = new TBinaryProtocol.Factory().getProtocol(socket);
        client = new TDorisExternalService.Client(protocol);
    }

    private TScanOpenParams openParams() {
        TScanOpenParams params = new TScanOpenParams();
        params.setCluster(DEFAULT_CLUSTER_NAME);
        params.setDatabase(partition.getDatabase());
        params.setTable(partition.getTable());
        params.setTabletIds(new ArrayList<Long>(partition.getTabletIds()));
        params.setOpaquedQueryPlan(partition.getOpaquedQueryPlan());
        params.setBatchSize(tableConfig.getBatchSize());
        params.setQueryTimeout(config.getQueryTimeoutSec());
        params.setMemLimit(tableConfig.getExecMemLimit());
        params.setUser(config.getUsername());
        params.setPasswd(config.getPassword());
        return params;
    }

    private void fetchNextBatch() throws Exception {
        TScanNextBatchParams params = new TScanNextBatchParams();
        params.setContextId(contextId);
        params.setOffset(readerOffset);

        Exception lastFailure = null;
        int attempts = Math.max(1, config.getRequestRetries() + 1);
        for (int attempt = 0; attempt < attempts; attempt++) {
            try {
                TScanBatchResult result = client.getNext(params);
                ensureOk("getNext", result == null ? null : result.getStatus());
                if (result == null) {
                    throw new IOException("Doris BE getNext returned null result");
                }
                eos = result.isEos();
                byte[] payload = result.getRows();
                if (!eos && (payload == null || payload.length == 0)) {
                    throw new IOException(
                            "Doris BE getNext returned an empty Arrow payload before EOS: be="
                                    + partition.getBeAddress()
                                    + ", contextId="
                                    + contextId
                                    + ", offset="
                                    + readerOffset);
                }
                currentRows =
                        payload == null || payload.length == 0
                                ? Collections.<FluxRow>emptyList()
                                : DorisArrowRowReader.read(payload, rowType);
                currentRowIndex = 0;
                readerOffset += currentRows.size();
                return;
            } catch (TException failure) {
                lastFailure = failure;
                LOG.warn(
                        "Doris BE getNext failed: be={}, contextId={}, offset={}, attempt={}/{}, error={}",
                        partition.getBeAddress(),
                        contextId,
                        readerOffset,
                        attempt + 1,
                        attempts,
                        failure.getMessage());
            }
        }
        throw new IOException(
                "Doris BE getNext failed after retries: be="
                        + partition.getBeAddress()
                        + ", contextId="
                        + contextId
                        + ", offset="
                        + readerOffset,
                lastFailure);
    }

    private static void ensureOk(String operation, TStatus status) throws IOException {
        if (status == null || status.getStatusCode() != TStatusCode.OK) {
            throw new IOException(
                    "Doris BE "
                            + operation
                            + " failed: status="
                            + (status == null ? "null" : status.getStatusCode())
                            + ", errors="
                            + (status == null ? "[]" : status.getErrorMsgs()));
        }
    }

    static String[] parseBeAddress(String address) {
        if (address == null) {
            throw new IllegalArgumentException("BE address must not be null");
        }
        String value = address.trim();
        int separator = value.lastIndexOf(':');
        if (separator <= 0 || separator == value.length() - 1) {
            throw new IllegalArgumentException(
                    "Illegal Doris BE address, expected host:port: " + address);
        }
        String port = value.substring(separator + 1);
        int parsedPort;
        try {
            parsedPort = Integer.parseInt(port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Illegal Doris BE port: " + address, e);
        }
        if (parsedPort <= 0 || parsedPort > 65535) {
            throw new IllegalArgumentException("Doris BE port must be between 1 and 65535: " + address);
        }
        return new String[] {value.substring(0, separator), port};
    }

    private void closeTransport() {
        if (socket != null) {
            socket.close();
        }
        client = null;
        socket = null;
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        if (client != null && contextId != null) {
            TScanCloseParams params = new TScanCloseParams();
            params.setContextId(contextId);
            try {
                TScanCloseResult result = client.closeScanner(params);
                ensureOk("closeScanner", result == null ? null : result.getStatus());
            } catch (Exception e) {
                failure = e;
                LOG.warn(
                        "Failed to close Doris scanner: be={}, contextId={}",
                        partition.getBeAddress(),
                        contextId,
                        e);
            }
        }
        closeTransport();
        contextId = null;
        currentRows = Collections.emptyList();
        currentRowIndex = 0;
        if (failure != null) {
            throw failure;
        }
    }
}
