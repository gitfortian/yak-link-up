package com.link.up.connector.file.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceEnumeratorContext;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.file.config.FileSourceConfig;

import java.util.Map;

/** Bounded file Source over local filesystem or S3. */
public final class FileSource implements Source<FileSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final FileSourceConfig config;

    public FileSource(FileSourceConfig config) {
        this.config = config;
    }

    @Override
    public FileSourceSplitEnumerator createEnumerator(
            Map<TablePath, CatalogTable> tables,
            SourceEnumeratorContext context) {

        return new FileSourceSplitEnumerator(config);
    }

    @Override
    public SourceReader<FluxRow, FileSourceSplit> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize) {

        return new FileSourceReader(config, tables, batchSize);
    }
}
