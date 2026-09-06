package com.link.up.connector.elasticsearch7.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SourceConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Bounded Elasticsearch 7 Source backed by scroll/sliced-scroll reads. */
public final class Elasticsearch7Source implements Source<Elasticsearch7SourceSplit> {

    private static final long serialVersionUID = 1L;

    private final Elasticsearch7SourceConfig config;

    public Elasticsearch7Source(Elasticsearch7SourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public SourceSplitEnumerator<Elasticsearch7SourceSplit> createEnumerator(
            Map<TablePath, CatalogTable> tables,
            SourceEnumeratorContext context) {
        Objects.requireNonNull(tables, "tables must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return new Elasticsearch7SourceSplitEnumerator(config, context.getParallelism());
    }

    @Override
    @Deprecated
    public List<Elasticsearch7SourceSplit> createSplits(
            Map<TablePath, CatalogTable> tables) throws Exception {
        Objects.requireNonNull(tables, "tables must not be null");
        try (Elasticsearch7SourceSplitEnumerator enumerator =
                     new Elasticsearch7SourceSplitEnumerator(config, 1)) {
            return enumerator.enumerateSplits();
        }
    }

    @Override
    public SourceReader<FluxRow, Elasticsearch7SourceSplit> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        return new Elasticsearch7SourceReader(config, tables, batchSize);
    }

    public Elasticsearch7SourceConfig getConfig() {
        return config;
    }
}
