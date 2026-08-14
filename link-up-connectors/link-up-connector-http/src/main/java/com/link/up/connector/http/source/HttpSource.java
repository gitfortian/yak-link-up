package com.link.up.connector.http.source;

import com.link.up.api.source.Source;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.http.config.HttpSourceConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * HTTP 离线数据源。
 *
 * <p>HTTP Source 只生成一个分片，分页由 Reader 内部处理。
 */
public final class HttpSource implements Source<HttpSourceSplit> {

    private static final long serialVersionUID = 1L;

    private final HttpSourceConfig config;

    public HttpSource(HttpSourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public List<HttpSourceSplit> createSplits(
            Map<TablePath, CatalogTable> tables) {
        return Collections.singletonList(new HttpSourceSplit());
    }

    @Override
    public SourceReader<FluxRow, HttpSourceSplit> createReader(
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        return new HttpSourceReader(config, tables, batchSize);
    }

    public HttpSourceConfig getConfig() {
        return config;
    }
}
