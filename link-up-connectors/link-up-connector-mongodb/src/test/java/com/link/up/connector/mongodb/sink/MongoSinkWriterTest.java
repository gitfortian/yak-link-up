package com.link.up.connector.mongodb.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.sink.CommitScope;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.mongodb.config.MongoSinkConfig;
import org.bson.BsonDocument;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoSinkWriterTest {

    @Test
    public void flushesAtBatchThresholdAndPrepareCommitFlushesTail() throws Exception {
        CatalogTable source = sourceTable();
        FakeInsertExecutor executor = new FakeInsertExecutor();
        MongoSinkWriter writer = writer(source, executor, 2);

        writer.open();
        writer.write(batch(FluxRow.of(Long.valueOf(1L), "one"), FluxRow.of(Long.valueOf(2L), "two")), source);
        assertEquals(1, executor.requests.size());
        assertEquals(2, executor.requests.get(0).size());
        assertEquals(0, writer.getBufferedDocuments());

        writer.write(batch(FluxRow.of(Long.valueOf(3L), "three")), source);
        assertEquals(1, writer.getBufferedDocuments());
        writer.prepareCommit();

        assertEquals(2, executor.requests.size());
        assertEquals(1, executor.requests.get(1).size());
        assertEquals(3L, writer.getTotalWrittenRows());
        assertEquals(2L, writer.getTotalInsertRequests());
        assertEquals(CommitScope.TASK_LOCAL, writer.getCommitScope());
        writer.close();
    }

    @Test
    public void closeDoesNotImplicitlyFlush() throws Exception {
        CatalogTable source = sourceTable();
        FakeInsertExecutor executor = new FakeInsertExecutor();
        MongoSinkWriter writer = writer(source, executor, 10);

        writer.open();
        writer.write(batch(FluxRow.of(Long.valueOf(1L), "one")), source);
        writer.close();

        assertTrue(executor.requests.isEmpty());
        assertEquals(1, executor.closeCount);
    }

    @Test
    public void abortDiscardsOnlyUnsentBuffer() throws Exception {
        CatalogTable source = sourceTable();
        FakeInsertExecutor executor = new FakeInsertExecutor();
        MongoSinkWriter writer = writer(source, executor, 10);

        writer.open();
        writer.write(batch(FluxRow.of(Long.valueOf(1L), "one")), source);
        writer.abort();

        assertEquals(0, writer.getBufferedDocuments());
        assertTrue(executor.requests.isEmpty());
        writer.close();
    }

    @Test
    public void insertFailureStopsWriterInsteadOfReplaying() throws Exception {
        CatalogTable source = sourceTable();
        FakeInsertExecutor executor = new FakeInsertExecutor();
        executor.failInsert = true;
        MongoSinkWriter writer = writer(source, executor, 2);

        writer.open();
        try {
            writer.write(batch(FluxRow.of(Long.valueOf(1L), "one"), FluxRow.of(Long.valueOf(2L), "two")), source);
            fail("Expected MongoDB insert failure");
        } catch (IllegalStateException expected) {
            // expected
        }

        executor.failInsert = false;
        try {
            writer.prepareCommit();
            fail("Writer must remain failed after ambiguous insert outcome");
        } catch (IllegalStateException expected) {
            // expected
        }
        assertTrue(executor.requests.isEmpty());
        writer.abort();
        writer.close();
    }

    private static MongoSinkWriter writer(
            CatalogTable source,
            FakeInsertExecutor executor,
            int batchSize) {
        MongoSinkConfig config = config(batchSize);
        Map<TablePath, CatalogTable> targets = new LinkedHashMap<TablePath, CatalogTable>();
        targets.put(
                source.getTablePath(),
                CatalogTable.builder(config.getTargetPath(), source.getTableSchema()).build());
        return new MongoSinkWriter(
                config,
                new PreparedSinkMetadata(targets),
                executor);
    }

    private static CatalogTable sourceTable() {
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                .column(Column.builder("name", BasicType.STRING_TYPE).build())
                .build();
        return CatalogTable.builder(TablePath.of("source", "orders"), schema).build();
    }

    private static RecordBatch<FluxRow> batch(FluxRow... rows) {
        List<FluxRow> values = new ArrayList<FluxRow>();
        for (FluxRow row : rows) {
            values.add(row);
        }
        return RecordBatch.of("source.orders", "split-0", values);
    }

    private static MongoSinkConfig config(int batchSize) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("uri", "mongodb://localhost:27017/archive");
        values.put("collection", "orders_copy");
        values.put("batch_size", batchSize);
        return MongoSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static final class FakeInsertExecutor
            implements MongoSinkWriter.BatchInsertExecutor {

        private final List<List<BsonDocument>> requests =
                new ArrayList<List<BsonDocument>>();
        private int closeCount;
        private boolean failInsert;

        @Override
        public void open() {
        }

        @Override
        public void insert(List<BsonDocument> documents) {
            if (failInsert) {
                throw new IllegalStateException("ambiguous insert failure");
            }
            requests.add(new ArrayList<BsonDocument>(documents));
        }

        @Override
        public void close() {
            closeCount++;
        }
    }
}
