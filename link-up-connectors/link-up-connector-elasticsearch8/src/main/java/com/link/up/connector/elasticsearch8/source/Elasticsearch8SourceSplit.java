package com.link.up.connector.elasticsearch8.source;

import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.TablePath;

import java.util.Objects;

/** One bounded Elasticsearch 8 sliced-scroll unit. */
public final class Elasticsearch8SourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final String index;
    private final int sliceId;
    private final int sliceMax;
    private final String splitId;

    public Elasticsearch8SourceSplit(String index, int sliceId, int sliceMax) {
        this.index = requireText(index, "index");
        if (sliceMax <= 0) {
            throw new IllegalArgumentException("sliceMax must be greater than 0");
        }
        if (sliceId < 0 || sliceId >= sliceMax) {
            throw new IllegalArgumentException("sliceId must be in [0, sliceMax)");
        }
        this.sliceId = sliceId;
        this.sliceMax = sliceMax;
        this.splitId = index + "#slice-" + sliceId + "-of-" + sliceMax;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String dataSetId() {
        return index;
    }

    public String getIndex() { return index; }
    public int getSliceId() { return sliceId; }
    public int getSliceMax() { return sliceMax; }
    public TablePath getTablePath() { return TablePath.of(index); }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return normalized;
    }

    @Override
    public String toString() {
        return "Elasticsearch8SourceSplit{index='" + index + '\''
                + ", sliceId=" + sliceId
                + ", sliceMax=" + sliceMax
                + '}';
    }
}
