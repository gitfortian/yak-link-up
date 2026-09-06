package com.link.up.connector.doris.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.doris.config.DorisSourceConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Doris bounded Source using FE query plans and direct BE Thrift/Arrow scans. */
public final class DorisSource implements Source<DorisSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final DorisSourceConfig config;

    public DorisSource(DorisSourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public SourceSplitEnumerator<DorisSourceSplit> createEnumerator(
            Map<TablePath, CatalogTable> tables,
            SourceEnumeratorContext context) {
        Objects.requireNonNull(tables, "tables must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return new DorisSourceSplitEnumerator(config, tables);
    }

    @Override
    @Deprecated
    public List<DorisSourceSplit> createSplits(
            Map<TablePath, CatalogTable> tables) throws Exception {
        try (DorisSourceSplitEnumerator enumerator =
                     new DorisSourceSplitEnumerator(config, tables)) {
            return enumerator.enumerateSplits();
        }
    }

    @Override
    public SourceReader<FluxRow, DorisSourceSplit> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        return new DorisSourceReader(config, tables, batchSize);
    }

    public DorisSourceConfig getConfig() { return config; }
}
