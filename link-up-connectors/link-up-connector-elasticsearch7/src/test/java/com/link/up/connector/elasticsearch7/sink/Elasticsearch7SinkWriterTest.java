package com.link.up.connector.elasticsearch7.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SinkConfig;
import com.link.up.connector.elasticsearch7.converter.Elasticsearch7DocumentConverter.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Elasticsearch7SinkWriterTest {

    @Test
    public void flushesAtBatchSizeAndPrepareCommit() throws Exception {
        CatalogTable source = sourceTable();
        PreparedSinkMetadata metadata = metadata(source);
        FakeBulkExecutor executor = new FakeBulkExecutor();
        Elasticsearch7SinkWriter writer =
                new Elasticsearch7SinkWriter(config(2), metadata, executor);

        writer.open();
        writer.write(
                RecordBatch.of(
                        "orders",
                        "split-0",
                        Arrays.asList(
                                FluxRow.of("1", 10L),
                                FluxRow.of("2", 20L),
                                FluxRow.of("3", 30L))),
                source);
        assertEquals(1, executor.executions.size());
        assertEquals(2, executor.executions.get(0).size());

        writer.prepareCommit();
        assertEquals(2, executor.executions.size());
        assertEquals(1, executor.executions.get(1).size());
        writer.commit();
        writer.close();
    }

    @Test
    public void closeDoesNotImplicitlyFlushPendingDocuments() throws Exception {
        CatalogTable source = sourceTable();
        FakeBulkExecutor executor = new FakeBulkExecutor();
        Elasticsearch7SinkWriter writer =
                new Elasticsearch7SinkWriter(config(2), metadata(source), executor);

        writer.open();
        writer.write(
                RecordBatch.of(
                        "orders",
                        "split-0",
                        Arrays.asList(FluxRow.of("1", 10L))),
                source);
        writer.close();

        assertEquals(0, executor.executions.size());
    }

    private static Elasticsearch7SinkConfig config(int batchSize) {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("hosts", Arrays.asList("http://localhost:9200"));
        values.put("index", "orders_target");
        values.put("document_id_field", "id");
        values.put("batch_size", batchSize);
        return Elasticsearch7SinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static CatalogTable sourceTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(Column.builder("id", BasicType.STRING_TYPE).sourceType("varchar").build())
                        .column(Column.builder("amount", BasicType.LONG_TYPE).sourceType("bigint").build())
                        .build();
        return CatalogTable.builder(TablePath.of("orders"), schema).build();
    }

    private static PreparedSinkMetadata metadata(CatalogTable source) {
        TableSchema targetSchema =
                TableSchema.builder()
                        .column(Column.builder("id", BasicType.STRING_TYPE).sourceType("keyword").build())
                        .column(Column.builder("amount", BasicType.LONG_TYPE).sourceType("long").build())
                        .build();
        CatalogTable target =
                CatalogTable.builder(TablePath.of("orders_target"), targetSchema).build();
        Map<TablePath, CatalogTable> targets =
                new LinkedHashMap<TablePath, CatalogTable>();
        targets.put(source.getTablePath(), target);
        return new PreparedSinkMetadata(targets);
    }

    private static final class FakeBulkExecutor
            implements Elasticsearch7SinkWriter.BulkExecutor {
        private final List<List<Document>> executions =
                new ArrayList<List<Document>>();

        @Override
        public void open() {
        }

        @Override
        public int execute(List<Document> documents) {
            executions.add(new ArrayList<Document>(documents));
            return documents.size();
        }

        @Override
        public void close() {
        }
    }
}
