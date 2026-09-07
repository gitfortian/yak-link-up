package com.link.up.connector.datagen.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.datagen.config.DataGenSourceConfig;

import java.util.Map;

/** Bounded in-memory data generator Source. */
public final class DataGenSource implements Source<DataGenSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final DataGenSourceConfig config;

    public DataGenSource(DataGenSourceConfig config) {
        this.config = config;
    }

    @Override
    public DataGenSourceSplitEnumerator createEnumerator(
            Map<TablePath, CatalogTable> tables,
            SourceEnumeratorContext context) {

        return new DataGenSourceSplitEnumerator(config, context.getParallelism());
    }

    @Override
    public SourceReader<FluxRow, DataGenSourceSplit> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize) {

        return new DataGenSourceReader(config, batchSize);
    }
}
