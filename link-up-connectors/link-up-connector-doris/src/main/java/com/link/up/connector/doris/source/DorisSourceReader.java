package com.link.up.connector.doris.source;

import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.doris.client.source.DorisBeReadClient;
import com.link.up.connector.doris.config.DorisSourceConfig;
import com.link.up.connector.doris.config.DorisSourceTableConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Reads assigned Doris native splits through task-local BE scanner clients. */
public final class DorisSourceReader
        implements SourceReader<FluxRow, DorisSourceSplit> {

    private final DorisSourceConfig config;
    private final Map<TablePath, CatalogTable> tables;
    private final int frameworkBatchSize;

    private List<DorisSourceSplit> splits = Collections.emptyList();
    private int splitIndex;
    private DorisSourceSplit currentSplit;
    private DorisBeReadClient currentClient;
    private boolean opened;
    private boolean finished;

    public DorisSourceReader(
            DorisSourceConfig config,
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.tables = Objects.requireNonNull(tables, "tables must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        this.frameworkBatchSize = batchSize;
    }

    @Override
    public void open(List<DorisSourceSplit> splits) throws Exception {
        if (opened) {
            throw new IllegalStateException("DorisSourceReader has already been opened");
        }
        if (splits == null) {
            throw new IllegalArgumentException("splits must not be null");
        }
        this.splits = Collections.unmodifiableList(new ArrayList<DorisSourceSplit>(splits));
        splitIndex = 0;
        currentSplit = null;
        currentClient = null;
        finished = false;
        opened = true;
    }

    @Override
    public void open() throws Exception {
        open(Collections.<DorisSourceSplit>emptyList());
    }

    @Override
    public void openSplit(DorisSourceSplit split) throws Exception {
        checkOpened();
        if (currentSplit != null) {
            throw new IllegalStateException("A Doris source split is already open");
        }
        finished = false;
        openCurrentSplit(Objects.requireNonNull(split, "split must not be null"));
    }

    @Override
    public void closeSplit() throws Exception {
        closeCurrentSplit();
    }

    @Override
    public RecordBatch<FluxRow> readBatch() throws Exception {
        checkOpened();
        if (finished) {
            return RecordBatch.endOfInput();
        }
        while (true) {
            if (currentSplit == null) {
                if (!openNextSplit()) {
                    finished = true;
                    return RecordBatch.endOfInput();
                }
            }
            DorisSourceSplit batchSplit = currentSplit;
            List<FluxRow> rows = new ArrayList<FluxRow>(frameworkBatchSize);
            boolean exhausted = false;
            while (rows.size() < frameworkBatchSize) {
                if (!currentClient.hasNext()) {
                    exhausted = true;
                    break;
                }
                rows.add(currentClient.next());
            }
            if (exhausted) {
                closeCurrentSplit();
            }
            if (!rows.isEmpty()) {
                return RecordBatch.of(batchSplit, rows);
            }
        }
    }

    private boolean openNextSplit() throws Exception {
        if (splitIndex >= splits.size()) {
            return false;
        }
        openCurrentSplit(splits.get(splitIndex++));
        return true;
    }

    private void openCurrentSplit(DorisSourceSplit split) throws Exception {
        CatalogTable table = tables.get(split.getTablePath());
        if (table == null) {
            throw new IllegalArgumentException(
                    "Cannot find schema for Doris split table: " + split.getTablePath());
        }
        DorisSourceTableConfig tableConfig = findTableConfig(split.getTablePath());
        DorisBeReadClient client =
                new DorisBeReadClient(config, tableConfig, split.getPartition(), table.getRowType());
        try {
            client.open();
        } catch (Exception failure) {
            try {
                client.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
        currentSplit = split;
        currentClient = client;
    }

    private DorisSourceTableConfig findTableConfig(TablePath tablePath) {
        for (DorisSourceTableConfig tableConfig : config.getTableConfigs()) {
            if (tableConfig.getTablePath().equals(tablePath)) {
                return tableConfig;
            }
        }
        throw new IllegalArgumentException("Cannot find Doris table config for: " + tablePath);
    }

    private void closeCurrentSplit() throws Exception {
        if (currentClient == null) {
            currentSplit = null;
            return;
        }
        try {
            currentClient.close();
        } finally {
            currentClient = null;
            currentSplit = null;
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("DorisSourceReader has not been opened");
        }
    }

    @Override
    public void close() throws Exception {
        if (!opened) {
            return;
        }
        try {
            closeCurrentSplit();
        } finally {
            opened = false;
            finished = true;
            splits = Collections.emptyList();
        }
    }
}
