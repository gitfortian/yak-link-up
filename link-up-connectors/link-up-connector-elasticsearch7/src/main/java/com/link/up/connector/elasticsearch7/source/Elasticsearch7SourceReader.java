package com.link.up.connector.elasticsearch7.source;

import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.elasticsearch7.client.Elasticsearch7Client;
import com.link.up.connector.elasticsearch7.client.Elasticsearch7Client.ScrollPage;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SourceConfig;
import com.link.up.connector.elasticsearch7.converter.Elasticsearch7RowConverter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Bounded ES7 reader with one active scroll context per split. */
public final class Elasticsearch7SourceReader
        implements SourceReader<FluxRow, Elasticsearch7SourceSplit> {

    private final Elasticsearch7SourceConfig config;
    private final Map<TablePath, CatalogTable> tables;
    private final int batchSize;

    private Elasticsearch7Client client;
    private List<Elasticsearch7SourceSplit> assignedSplits = Collections.emptyList();
    private int nextSplitIndex;
    private Elasticsearch7SourceSplit currentSplit;
    private Elasticsearch7RowConverter rowConverter;
    private ScrollPage currentPage;
    private int currentPageOffset;
    private String currentScrollId;
    private boolean opened;
    private boolean closed;

    public Elasticsearch7SourceReader(
            Elasticsearch7SourceConfig config,
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(tables, "tables must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        this.tables =
                Collections.unmodifiableMap(
                        new LinkedHashMap<TablePath, CatalogTable>(tables));
        this.batchSize = batchSize;
    }

    @Override
    public void open(List<Elasticsearch7SourceSplit> splits) throws Exception {
        ensureNotOpened();
        this.assignedSplits =
                splits == null
                        ? Collections.<Elasticsearch7SourceSplit>emptyList()
                        : Collections.unmodifiableList(
                                new ArrayList<Elasticsearch7SourceSplit>(splits));
        initializeClient();
    }

    @Override
    public void open() throws Exception {
        open(Collections.<Elasticsearch7SourceSplit>emptyList());
    }

    @Override
    public void openSplit(Elasticsearch7SourceSplit split) throws Exception {
        Objects.requireNonNull(split, "split must not be null");
        if (!opened) {
            open();
        }
        ensureUsable();
        if (currentSplit != null) {
            throw new IllegalStateException("A split is already open: " + currentSplit.splitId());
        }
        openCurrentSplit(split);
    }

    @Override
    public RecordBatch<FluxRow> readBatch() throws Exception {
        ensureUsable();

        while (true) {
            if (currentSplit == null && !openNextAssignedSplit()) {
                return RecordBatch.endOfInput();
            }

            Elasticsearch7SourceSplit batchSplit = currentSplit;
            List<FluxRow> rows = new ArrayList<FluxRow>(batchSize);

            while (rows.size() < batchSize && currentSplit == batchSplit) {
                if (currentPageOffset < currentPage.getDocuments().size()) {
                    rows.add(
                            rowConverter.convert(
                                    currentPage.getDocuments().get(currentPageOffset++)));
                    continue;
                }

                if (currentPage.getDocuments().isEmpty()
                        || currentScrollId == null
                        || currentScrollId.trim().isEmpty()) {
                    closeSplit();
                    break;
                }

                ScrollPage nextPage = client.continueScroll(currentScrollId);
                currentPage = nextPage;
                currentPageOffset = 0;
                currentScrollId = nextPage.getScrollId();
                if (nextPage.getDocuments().isEmpty()) {
                    closeSplit();
                    break;
                }
            }

            if (!rows.isEmpty()) {
                return RecordBatch.of(batchSplit, rows);
            }
        }
    }

    private boolean openNextAssignedSplit() throws Exception {
        while (nextSplitIndex < assignedSplits.size()) {
            Elasticsearch7SourceSplit split = assignedSplits.get(nextSplitIndex++);
            openCurrentSplit(split);
            return true;
        }
        return false;
    }

    private void openCurrentSplit(Elasticsearch7SourceSplit split) throws Exception {
        CatalogTable table = tables.get(split.getTablePath());
        if (table == null) {
            throw new IllegalArgumentException(
                    "No prepared schema found for Elasticsearch index: " + split.getTablePath());
        }
        this.currentSplit = split;
        this.rowConverter = new Elasticsearch7RowConverter(table.getTableSchema());
        this.currentPage = client.startScroll(split);
        this.currentPageOffset = 0;
        this.currentScrollId = currentPage.getScrollId();
    }

    @Override
    public void closeSplit() throws Exception {
        String scrollId = currentScrollId;
        currentSplit = null;
        rowConverter = null;
        currentPage = null;
        currentPageOffset = 0;
        currentScrollId = null;
        if (scrollId != null && client != null) {
            client.clearScroll(scrollId);
        }
    }

    private void initializeClient() throws Exception {
        this.client = new Elasticsearch7Client(config);
        boolean success = false;
        try {
            client.verifyMajorVersion();
            opened = true;
            success = true;
        } finally {
            if (!success) {
                client.close();
                client = null;
            }
        }
    }

    private void ensureNotOpened() {
        if (opened || closed) {
            throw new IllegalStateException("Elasticsearch7SourceReader has already been opened or closed");
        }
    }

    private void ensureUsable() {
        if (!opened || closed || client == null) {
            throw new IllegalStateException("Elasticsearch7SourceReader is not open");
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        Exception failure = null;
        try {
            if (currentSplit != null || currentScrollId != null) {
                closeSplit();
            }
        } catch (Exception closeFailure) {
            failure = closeFailure;
        }
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        } finally {
            client = null;
            opened = false;
            closed = true;
        }
        if (failure != null) {
            throw failure;
        }
    }
}
