package com.link.up.connector.mongodb.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.mongodb.config.MongoSourceConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Bounded MongoDB Source using one finite collection cursor. */
public final class MongoSource implements Source<MongoSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final MongoSourceConfig config;

    public MongoSource(MongoSourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public SourceSplitEnumerator<MongoSourceSplit> createEnumerator(
            Map<TablePath, CatalogTable> tables,
            SourceEnumeratorContext context) {
        Objects.requireNonNull(tables, "tables must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return new MongoSourceSplitEnumerator(config);
    }

    @Override
    @Deprecated
    public List<MongoSourceSplit> createSplits(Map<TablePath, CatalogTable> tables) {
        Objects.requireNonNull(tables, "tables must not be null");
        return new MongoSourceSplitEnumerator(config).enumerateSplits();
    }

    @Override
    public SourceReader<FluxRow, MongoSourceSplit> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        return new MongoSourceReader(config, tables, batchSize);
    }

    public MongoSourceConfig getConfig() {
        return config;
    }
}
