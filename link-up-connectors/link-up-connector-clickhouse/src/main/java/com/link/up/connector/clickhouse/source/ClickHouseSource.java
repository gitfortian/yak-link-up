package com.link.up.connector.clickhouse.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.clickhouse.config.ClickHouseSourceConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** ClickHouse bounded Source using active-part splits and official JDBC/HTTP reads. */
public final class ClickHouseSource implements Source<ClickHouseSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final ClickHouseSourceConfig config;

    public ClickHouseSource(ClickHouseSourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public SourceSplitEnumerator<ClickHouseSourceSplit> createEnumerator(
            Map<TablePath, CatalogTable> tables,
            SourceEnumeratorContext context) {
        Objects.requireNonNull(tables, "tables must not be null");
        Objects.requireNonNull(context, "context must not be null");
        return new ClickHouseSourceSplitEnumerator(config);
    }

    @Override
    @Deprecated
    public List<ClickHouseSourceSplit> createSplits(
            Map<TablePath, CatalogTable> tables) throws Exception {
        try (ClickHouseSourceSplitEnumerator enumerator =
                     new ClickHouseSourceSplitEnumerator(config)) {
            return enumerator.enumerateSplits();
        }
    }

    @Override
    public SourceReader<FluxRow, ClickHouseSourceSplit> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        return new ClickHouseSourceReader(config, tables, batchSize);
    }

    public ClickHouseSourceConfig getConfig() {
        return config;
    }
}
