package com.link.up.connector.datagen.source;

import com.link.up.api.source.SourceSplit;

import java.util.Objects;

/** One bounded row-range generation unit. */
public final class DataGenSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final String dataSetId;
    private final long startRow;
    private final long rowCount;

    public DataGenSourceSplit(
            int sequence,
            String dataSetId,
            long startRow,
            long rowCount) {

        this.splitId = "datagen#" + sequence;
        this.dataSetId = Objects.requireNonNull(dataSetId, "dataSetId must not be null");
        if (startRow < 0) {
            throw new IllegalArgumentException("startRow must not be negative");
        }
        if (rowCount <= 0) {
            throw new IllegalArgumentException("rowCount must be greater than 0");
        }
        this.startRow = startRow;
        this.rowCount = rowCount;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String dataSetId() {
        return dataSetId;
    }

    public long getStartRow() {
        return startRow;
    }

    public long getRowCount() {
        return rowCount;
    }

    @Override
    public String toString() {
        return "DataGenSourceSplit{"
                + "splitId='" + splitId + '\''
                + ", dataSetId='" + dataSetId + '\''
                + ", startRow=" + startRow
                + ", rowCount=" + rowCount
                + '}';
    }
}
