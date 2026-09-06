package com.link.up.connector.elasticsearch8.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.elasticsearch8.config.Elasticsearch8SourceConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Elasticsearch 8 bounded Source using sliced Scroll reads. */
public final class Elasticsearch8Source implements Source<Elasticsearch8SourceSplit> {

    private static final long serialVersionUID = 1L;

    private final Elasticsearch8SourceConfig config;

    public Elasticsearch8Source(Elasticsearch8SourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public SourceSplitEnumerator<Elasticsearch8SourceSplit> createEnumerator(
            Map<TablePath, CatalogTable> tables,
            SourceEnumeratorContext context) {
        Objects.requireNonNull(tables, "tables must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return new Elasticsearch8SourceSplitEnumerator(config, context.getParallelism());
    }

    @Override
    @Deprecated
    public List<Elasticsearch8SourceSplit> createSplits(
            Map<TablePath, CatalogTable> tables,
            int parallelism) throws Exception {
        try (Elasticsearch8SourceSplitEnumerator enumerator =
                     new Elasticsearch8SourceSplitEnumerator(config, parallelism)) {
            return enumerator.enumerateSplits();
        }
    }

    @Override
    public SourceReader<FluxRow, Elasticsearch8SourceSplit> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        return new Elasticsearch8SourceReader(config, tables, batchSize);
    }

    public Elasticsearch8SourceConfig getConfig() {
        return config;
    }
}
