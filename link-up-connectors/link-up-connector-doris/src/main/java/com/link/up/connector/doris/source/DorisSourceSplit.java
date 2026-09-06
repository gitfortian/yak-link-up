package com.link.up.connector.doris.source;

import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.doris.client.source.model.DorisQueryPartition;

import java.util.Objects;

/** A deterministic Link-Up split for one Doris BE native scan partition. */
public final class DorisSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final String splitId;
    private final TablePath tablePath;
    private final DorisQueryPartition partition;

    public DorisSourceSplit(
            String splitId,
            TablePath tablePath,
            DorisQueryPartition partition) {
        this.splitId = Objects.requireNonNull(splitId, "splitId must not be null");
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath must not be null");
        this.partition = Objects.requireNonNull(partition, "partition must not be null");
    }

    @Override
    public String splitId() { return splitId; }

    @Override
    public String dataSetId() { return tablePath.toString(); }

    public TablePath getTablePath() { return tablePath; }
    public DorisQueryPartition getPartition() { return partition; }

    @Override
    public String toString() {
        return "DorisSourceSplit{"
                + "splitId='" + splitId + '\''
                + ", tablePath=" + tablePath
                + ", be=" + partition.getBeAddress()
                + ", tablets=" + partition.getTabletIds()
                + '}';
    }
}
