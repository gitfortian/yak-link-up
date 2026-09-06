package com.link.up.connector.elasticsearch7.source;

import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.TablePath;

import java.util.Objects;

/** One bounded Elasticsearch sliced-scroll read unit. */
public final class Elasticsearch7SourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final String index;
    private final int sliceId;
    private final int sliceMax;
    private final String splitId;

    public Elasticsearch7SourceSplit(String index, int sliceId, int sliceMax) {
        String normalizedIndex = requireText(index, "index");
        if (sliceMax <= 0) {
            throw new IllegalArgumentException("sliceMax must be greater than 0");
        }
        if (sliceId < 0 || sliceId >= sliceMax) {
            throw new IllegalArgumentException("sliceId must be in [0, sliceMax)");
        }
        this.index = normalizedIndex;
        this.tablePath = TablePath.of(normalizedIndex);
        this.sliceId = sliceId;
        this.sliceMax = sliceMax;
        this.splitId = normalizedIndex + ":slice-" + sliceId + "-of-" + sliceMax;
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

    public String getIndex() {
        return index;
    }

    public int getSliceId() {
        return sliceId;
    }

    public int getSliceMax() {
        return sliceMax;
    }

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
        return "Elasticsearch7SourceSplit{"
                + "index='" + index + '\''
                + ", sliceId=" + sliceId
                + ", sliceMax=" + sliceMax
                + '}';
    }
}
