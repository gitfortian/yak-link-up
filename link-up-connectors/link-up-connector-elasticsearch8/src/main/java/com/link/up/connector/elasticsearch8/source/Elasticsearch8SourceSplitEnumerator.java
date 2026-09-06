package com.link.up.connector.elasticsearch8.source;

import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.connector.elasticsearch8.config.Elasticsearch8SourceConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Plans one sliced-scroll split per configured or execution parallel slice. */
public final class Elasticsearch8SourceSplitEnumerator
        implements SourceSplitEnumerator<Elasticsearch8SourceSplit> {

    private final Elasticsearch8SourceConfig config;
    private final int parallelism;

    public Elasticsearch8SourceSplitEnumerator(
            Elasticsearch8SourceConfig config,
            int parallelism) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be greater than 0");
        }
        this.parallelism = parallelism;
    }

    @Override
    public List<Elasticsearch8SourceSplit> enumerateSplits() {
        int sliceMax = config.resolveSlices(parallelism);
        List<Elasticsearch8SourceSplit> result =
                new ArrayList<Elasticsearch8SourceSplit>(sliceMax);
        for (int sliceId = 0; sliceId < sliceMax; sliceId++) {
            result.add(new Elasticsearch8SourceSplit(config.getIndex(), sliceId, sliceMax));
        }
        return Collections.unmodifiableList(result);
    }
}
