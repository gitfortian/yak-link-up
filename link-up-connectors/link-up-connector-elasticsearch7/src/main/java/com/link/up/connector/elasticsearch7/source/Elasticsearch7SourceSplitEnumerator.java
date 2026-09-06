package com.link.up.connector.elasticsearch7.source;

import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SourceConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Plans one sliced-scroll split per configured or execution parallel slice. */
public final class Elasticsearch7SourceSplitEnumerator
        implements SourceSplitEnumerator<Elasticsearch7SourceSplit> {

    private final Elasticsearch7SourceConfig config;
    private final int parallelism;

    public Elasticsearch7SourceSplitEnumerator(
            Elasticsearch7SourceConfig config,
            int parallelism) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be greater than 0");
        }
        this.parallelism = parallelism;
    }

    @Override
    public List<Elasticsearch7SourceSplit> enumerateSplits() {
        int sliceMax = config.resolveSlices(parallelism);
        List<Elasticsearch7SourceSplit> result =
                new ArrayList<Elasticsearch7SourceSplit>(sliceMax);
        for (int sliceId = 0; sliceId < sliceMax; sliceId++) {
            result.add(
                    new Elasticsearch7SourceSplit(
                            config.getIndex(),
                            sliceId,
                            sliceMax));
        }
        return Collections.unmodifiableList(result);
    }
}
