package com.link.up.connector.mongodb.source;

import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.TablePath;

import java.util.Objects;

/** One bounded full-collection scan unit. */
public final class MongoSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final String splitId;

    public MongoSourceSplit(TablePath tablePath) {
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath must not be null");
        this.splitId = tablePath.toString() + ":full-scan";
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String dataSetId() {
        return tablePath.toString();
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    @Override
    public String toString() {
        return "MongoSourceSplit{tablePath=" + tablePath + '}';
    }
}
