package com.link.up.connector.mongodb.source;

import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.mongodb.config.MongoSourceConfig;
import com.link.up.connector.mongodb.converter.MongoBsonRowConverter;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Bounded reader with one active MongoDB cursor per split. */
public final class MongoSourceReader implements SourceReader<FluxRow, MongoSourceSplit> {

    private final MongoSourceConfig config;
    private final Map<TablePath, CatalogTable> tables;
    private final int batchSize;

    private MongoClient client;
    private List<MongoSourceSplit> assignedSplits = Collections.emptyList();
    private int nextSplitIndex;
    private MongoSourceSplit currentSplit;
    private MongoCursor<BsonDocument> cursor;
    private MongoBsonRowConverter rowConverter;
    private boolean opened;
    private boolean closed;

    public MongoSourceReader(
            MongoSourceConfig config,
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
    public void open(List<MongoSourceSplit> splits) {
        ensureNotOpened();
        this.assignedSplits = splits == null
                ? Collections.<MongoSourceSplit>emptyList()
                : Collections.unmodifiableList(new ArrayList<MongoSourceSplit>(splits));
        initializeClient();
    }

    @Override
    public void open() {
        open(Collections.<MongoSourceSplit>emptyList());
    }

    @Override
    public void openSplit(MongoSourceSplit split) {
        Objects.requireNonNull(split, "split must not be null");
        if (!opened) {
            open();
        }
        ensureUsable();
        if (currentSplit != null) {
            throw new IllegalStateException("A MongoDB split is already open: " + currentSplit.splitId());
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

            MongoSourceSplit batchSplit = currentSplit;
            List<FluxRow> rows = new ArrayList<FluxRow>(batchSize);
            while (rows.size() < batchSize && currentSplit == batchSplit) {
                if (cursor.hasNext()) {
                    rows.add(rowConverter.convert(cursor.next()));
                    continue;
                }
                closeSplit();
            }

            if (!rows.isEmpty()) {
                return RecordBatch.of(batchSplit, rows);
            }
        }
    }

    private boolean openNextAssignedSplit() {
        if (nextSplitIndex >= assignedSplits.size()) {
            return false;
        }
        openCurrentSplit(assignedSplits.get(nextSplitIndex++));
        return true;
    }

    private void openCurrentSplit(MongoSourceSplit split) {
        if (!config.getTablePath().equals(split.getTablePath())) {
            throw new IllegalArgumentException(
                    "MongoDB split does not belong to the configured collection: " + split.getTablePath());
        }

        CatalogTable table = tables.get(split.getTablePath());
        if (table == null) {
            throw new IllegalArgumentException(
                    "No prepared schema found for MongoDB collection: " + split.getTablePath());
        }

        MongoBsonRowConverter preparedConverter =
                new MongoBsonRowConverter(table.getTableSchema());
        MongoCollection<BsonDocument> collection = client
                .getDatabase(config.getDatabase())
                .getCollection(config.getCollection(), BsonDocument.class);
        FindIterable<BsonDocument> find = collection
                .find(config.createFilter())
                .batchSize(config.getFetchSize());
        if (config.hasProjection()) {
            find = find.projection(MongoFieldProjection.build(config.getFields()));
        }

        MongoCursor<BsonDocument> openedCursor = find.iterator();
        this.rowConverter = preparedConverter;
        this.currentSplit = split;
        this.cursor = openedCursor;
    }

    @Override
    public void closeSplit() {
        MongoCursor<BsonDocument> currentCursor = cursor;
        cursor = null;
        currentSplit = null;
        rowConverter = null;
        if (currentCursor != null) {
            currentCursor.close();
        }
    }

    private void initializeClient() {
        MongoClient openedClient = MongoClients.create(config.getUri());
        boolean success = false;
        try {
            openedClient.getDatabase(config.getDatabase())
                    .runCommand(new BsonDocument("ping", new BsonInt32(1)));
            this.client = openedClient;
            this.opened = true;
            success = true;
        } finally {
            if (!success) {
                openedClient.close();
            }
        }
    }

    private void ensureNotOpened() {
        if (opened || closed) {
            throw new IllegalStateException("MongoSourceReader has already been opened or closed");
        }
    }

    private void ensureUsable() {
        if (!opened || closed || client == null) {
            throw new IllegalStateException("MongoSourceReader is not open");
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
            if (client != null) {
                client.close();
            }
            client = null;
            opened = false;
            closed = true;
        }
    }
}
