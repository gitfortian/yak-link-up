package com.link.up.connector.mongodb.source;

import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.connector.mongodb.config.MongoSourceConfig;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Stage 2 intentionally emits exactly one bounded collection split. */
public final class MongoSourceSplitEnumerator
        implements SourceSplitEnumerator<MongoSourceSplit> {

    private final MongoSourceConfig config;

    public MongoSourceSplitEnumerator(MongoSourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public List<MongoSourceSplit> enumerateSplits() {
        return Collections.singletonList(new MongoSourceSplit(config.getTablePath()));
    }
}
