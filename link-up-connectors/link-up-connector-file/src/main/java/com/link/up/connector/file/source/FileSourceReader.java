package com.link.up.connector.file.source;

import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.file.config.FileFormat;
import com.link.up.connector.file.config.FileSourceConfig;
import com.link.up.connector.file.converter.FileRowConverter;
import com.link.up.connector.file.converter.FileRowConverters;
import com.link.up.connector.file.internal.FileStorage;
import com.link.up.connector.file.internal.FileStorages;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

/**
 * Reads assigned splits one at a time; a split is a bounded byte range that
 * always starts on a row boundary. Header rows are skipped only in the split
 * starting at offset zero, because every continuation split starts exactly
 * after a row terminator.
 */
public final class FileSourceReader
        implements SourceReader<FluxRow, FileSourceSplit> {

    private final FileSourceConfig config;
    private final Map<TablePath, CatalogTable> tables;
    private final int batchSize;

    private FileStorage storage;
    private FileRowConverter converter;
    private List<FileSourceSplit> assignedSplits = Collections.emptyList();
    private int nextSplitIndex;
    private FileSourceSplit currentSplit;
    private BufferedReader lineReader;
    private long splitLocalLine;
    private boolean opened;
    private boolean closed;

    public FileSourceReader(
            FileSourceConfig config,
            Map<TablePath, CatalogTable> tables,
            int batchSize) {

        this.config = Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(tables, "tables must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        this.tables = Collections.unmodifiableMap(
                new LinkedHashMap<TablePath, CatalogTable>(tables));
        this.batchSize = batchSize;
    }

    @Override
    public void open(List<FileSourceSplit> splits) {
        ensureNotOpened();
        this.assignedSplits = Collections.unmodifiableList(
                new ArrayList<FileSourceSplit>(
                        splits == null
                                ? Collections.<FileSourceSplit>emptyList()
                                : splits));
        this.storage = FileStorages.create(config);
        this.converter = createConverter();
        this.opened = true;
    }

    @Override
    public void open() {
        open(Collections.<FileSourceSplit>emptyList());
    }

    @Override
    public void openSplit(FileSourceSplit split) {
        Objects.requireNonNull(split, "split must not be null");
        if (!opened) {
            open();
        }
        ensureUsable();
        if (currentSplit != null) {
            throw new IllegalStateException("A file split is already open: " + currentSplit.splitId());
        }
        openCurrentSplit(split);
    }

    @Override
    public RecordBatch<FluxRow> readBatch() {
        ensureUsable();

        while (true) {
            if (currentSplit == null && !openNextAssignedSplit()) {
                return RecordBatch.endOfInput();
            }
            FileSourceSplit batchSplit = currentSplit;

            List<FluxRow> rows = new ArrayList<FluxRow>(batchSize);
            while (rows.size() < batchSize) {
                String line;
                try {
                    line = lineReader.readLine();
                } catch (IOException failure) {
                    throw new IllegalStateException(
                            "Could not read from " + batchSplit.getFileKey()
                                    + " at split offset " + batchSplit.getStartOffset(),
                            failure);
                }
                if (line == null) {
                    closeSplit();
                    break;
                }
                splitLocalLine++;
                if (line.isEmpty() && config.getFormat() != FileFormat.TEXT) {
                    continue;
                }
                rows.add(converter.convert(line, rowContext(batchSplit)));
            }

            if (!rows.isEmpty()) {
                return RecordBatch.of(batchSplit, rows);
            }
        }
    }

    @Override
    public void closeSplit() {
        try {
            if (lineReader != null) {
                lineReader.close();
            }
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Could not close the split reader of "
                            + (currentSplit == null ? "unknown" : currentSplit.getFileKey()),
                    failure);
        } finally {
            lineReader = null;
            currentSplit = null;
            splitLocalLine = 0;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            closeSplit();
        } finally {
            if (storage != null) {
                storage.close();
            }
            storage = null;
            closed = true;
        }
    }

    private boolean openNextAssignedSplit() {
        if (nextSplitIndex >= assignedSplits.size()) {
            return false;
        }
        FileSourceSplit split = assignedSplits.get(nextSplitIndex++);
        openCurrentSplit(split);
        return true;
    }

    private void openCurrentSplit(FileSourceSplit split) {
        if (!config.getTableName().equals(split.dataSetId())) {
            throw new IllegalArgumentException(
                    "File split does not belong to the configured dataset: " + split.dataSetId());
        }
        if (config.isGzipFile(fileNameOf(split.getFileKey())) && split.getStartOffset() != 0) {
            throw new IllegalStateException(
                    "A gz file must be planned as one whole-file split, but offset is "
                            + split.getStartOffset() + " for " + split.getFileKey());
        }

        try {
            InputStream range = storage.openRange(
                    split.getFileKey(),
                    split.getStartOffset(),
                    split.getLength());
            InputStream input = config.isGzipFile(fileNameOf(split.getFileKey()))
                    ? new GZIPInputStream(range)
                    : range;
            this.lineReader = new BufferedReader(
                    new InputStreamReader(input, config.getEncoding()));
            this.currentSplit = split;
            this.splitLocalLine = 0;

            long skip = config.isHeader() ? 1L : config.getSkipHeaderRows();
            for (long i = 0; i < skip; i++) {
                if (lineReader.readLine() == null) {
                    break;
                }
                splitLocalLine++;
            }
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "Could not open split of " + split.getFileKey()
                            + " [" + split.getStartOffset() + ", " + split.getLength() + ")",
                    failure);
        }
    }

    private FileRowConverter createConverter() {
        CatalogTable table = tables.get(config.getTablePath());
        if (table == null) {
            throw new IllegalArgumentException(
                    "No prepared schema found for the file dataset: " + config.getTableName());
        }
        return FileRowConverters.create(config, table.getTableSchema());
    }

    private static String fileNameOf(String fileKey) {
        int nameStart = Math.max(fileKey.lastIndexOf('/'), fileKey.lastIndexOf('\\'));
        return nameStart < 0 ? fileKey : fileKey.substring(nameStart + 1);
    }

    private String rowContext(FileSourceSplit split) {
        return "in " + split.getFileKey()
                + " at split offset " + split.getStartOffset()
                + ", split-local line " + splitLocalLine;
    }

    private void ensureNotOpened() {
        if (opened || closed) {
            throw new IllegalStateException("FileSourceReader has already been opened or closed");
        }
    }

    private void ensureUsable() {
        if (!opened || closed) {
            throw new IllegalStateException("FileSourceReader is not open");
        }
    }
}
