package com.link.up.connector.datagen.source;

import com.link.up.connector.datagen.config.DataGenSourceConfig;
import com.link.up.api.source.SourceSplitEnumerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Splits the row range into deterministic bounded generation units. */
public final class DataGenSourceSplitEnumerator
        implements SourceSplitEnumerator<DataGenSourceSplit> {

    private final DataGenSourceConfig config;
    private final int parallelism;

    public DataGenSourceSplitEnumerator(
            DataGenSourceConfig config,
            int parallelism) {

        this.config = Objects.requireNonNull(config, "config must not be null");
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be greater than 0");
        }
        this.parallelism = parallelism;
    }

    @Override
    public List<DataGenSourceSplit> enumerateSplits() {
        long totalRows = config.getRowCount();

        int splitCount = config.getSplitCount() == null
                ? parallelism
                : config.getSplitCount();
        splitCount = (int) Math.min(splitCount, totalRows);
        splitCount = Math.max(splitCount, 1);

        long baseRows = totalRows / splitCount;
        long remainder = totalRows % splitCount;

        List<DataGenSourceSplit> splits = new ArrayList<DataGenSourceSplit>(splitCount);
        long startRow = 0L;
        for (int sequence = 0; sequence < splitCount; sequence++) {
            long rowCount = baseRows + (sequence < remainder ? 1L : 0L);
            splits.add(new DataGenSourceSplit(
                    sequence,
                    config.getTableName(),
                    startRow,
                    rowCount));
            startRow += rowCount;
        }
        return Collections.unmodifiableList(splits);
    }
}
