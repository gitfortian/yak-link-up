package com.link.up.connector.datagen.source;

import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.datagen.config.DataGenSourceConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Generates rows for the assigned splits, one active split at a time.
 *
 * <p>Batch values are pure functions of the global row index, so split
 * assignment and batch size never change the produced data set.
 */
public final class DataGenSourceReader
        implements SourceReader<FluxRow, DataGenSourceSplit> {

    private final DataGenSourceConfig config;
    private final int batchSize;

    private DataGenRowGenerator generator;
    private List<DataGenSourceSplit> assignedSplits = Collections.emptyList();
    private int nextSplitIndex;
    private DataGenSourceSplit currentSplit;
    private long currentRow;
    private long batchesEmitted;
    private boolean opened;
    private boolean closed;

    public DataGenSourceReader(
            DataGenSourceConfig config,
            int batchSize) {

        this.config = Objects.requireNonNull(config, "config must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        this.batchSize = batchSize;
    }

    @Override
    public void open(List<DataGenSourceSplit> splits) {
        if (opened || closed) {
            throw new IllegalStateException("DataGenSourceReader has already been opened or closed");
        }
        this.assignedSplits = Collections.unmodifiableList(
                new ArrayList<DataGenSourceSplit>(
                        splits == null
                                ? Collections.<DataGenSourceSplit>emptyList()
                                : splits));
        this.generator = new DataGenRowGenerator(config);
        this.opened = true;
    }

    @Override
    public void open() {
        open(Collections.<DataGenSourceSplit>emptyList());
    }

    @Override
    public void openSplit(DataGenSourceSplit split) {
        Objects.requireNonNull(split, "split must not be null");
        if (!opened) {
            open();
        }
        ensureUsable();
        if (currentSplit != null) {
            throw new IllegalStateException("A DataGen split is already open: " + currentSplit.splitId());
        }
        openCurrentSplit(split);
    }

    @Override
    public RecordBatch<FluxRow> readBatch() {
        ensureUsable();

        while (true) {
            if (currentSplit == null && !openNextAssignedSplit()) {
                return RecordBatch.endOfInput();
            }

            long remaining = currentSplit.getStartRow() + currentSplit.getRowCount() - currentRow;
            if (remaining <= 0) {
                closeSplit();
                continue;
            }

            if (config.getReadIntervalMillis() > 0 && batchesEmitted > 0) {
                sleepInterval();
            }

            int count = (int) Math.min((long) batchSize, remaining);
            List<FluxRow> rows = new ArrayList<FluxRow>(count);
            for (int i = 0; i < count; i++) {
                rows.add(generator.generate(currentRow++));
            }

            batchesEmitted++;
            return RecordBatch.of(currentSplit, rows);
        }
    }

    @Override
    public void closeSplit() {
        currentSplit = null;
    }

    @Override
    public void close() {
        closed = true;
        currentSplit = null;
        generator = null;
    }

    private boolean openNextAssignedSplit() {
        if (nextSplitIndex >= assignedSplits.size()) {
            return false;
        }
        openCurrentSplit(assignedSplits.get(nextSplitIndex++));
        return true;
    }

    private void openCurrentSplit(DataGenSourceSplit split) {
        if (!config.getTableName().equals(split.dataSetId())) {
            throw new IllegalArgumentException(
                    "DataGen split does not belong to the configured table: " + split.dataSetId());
        }
        this.currentSplit = split;
        this.currentRow = split.getStartRow();
    }

    private void sleepInterval() {
        try {
            Thread.sleep(config.getReadIntervalMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "DataGen read interrupted while waiting for the next batch",
                    interrupted);
        }
    }

    private void ensureUsable() {
        if (!opened || closed) {
            throw new IllegalStateException("DataGenSourceReader is not open");
        }
    }
}
