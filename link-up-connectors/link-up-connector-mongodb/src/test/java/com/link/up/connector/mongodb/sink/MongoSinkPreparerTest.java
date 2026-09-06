package com.link.up.connector.mongodb.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPrepareContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.mongodb.config.MongoSinkConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MongoSinkPreparerTest {

    @Test
    public void mapsOneSourceTableToConfiguredTarget() {
        CatalogTable source = sourceTable(
                TableSchema.builder()
                        .column(Column.builder("id", BasicType.LONG_TYPE).build())
                        .column(Column.builder("name", BasicType.STRING_TYPE).build())
                        .build());

        PreparedSinkMetadata metadata = new MongoSinkPreparer(config(null))
                .prepare(context(source));

        CatalogTable target = metadata.getTargetTable(source.getTablePath());
        assertEquals(TablePath.of("archive", "orders_copy"), target.getTablePath());
        assertEquals(source.getTableSchema(), target.getTableSchema());
    }

    @Test
    public void acceptsMongoDocumentParentAndDottedChild() {
        Column parent = Column.builder("address", BasicType.STRING_TYPE)
                .attribute("mongodb.bsonTypes", "DOCUMENT")
                .attribute("mongodb.encoding", "extended-json")
                .build();
        Column child = Column.builder("address.city", BasicType.STRING_TYPE).build();
        CatalogTable source = sourceTable(
                TableSchema.builder().column(parent).column(child).build());

        new MongoSinkPreparer(config(null)).prepare(context(source));
    }

    @Test
    public void rejectsOrdinaryParentAndDottedChild() {
        CatalogTable source = sourceTable(
                TableSchema.builder()
                        .column(Column.builder("address", BasicType.STRING_TYPE).build())
                        .column(Column.builder("address.city", BasicType.STRING_TYPE).build())
                        .build());

        assertPrepareFails(source, config(null));
    }

    @Test
    public void rejectsDocumentIdOverrideWhenSourceAlreadyHasId() {
        CatalogTable source = sourceTable(
                TableSchema.builder()
                        .column(Column.builder("_id", BasicType.STRING_TYPE).build())
                        .column(Column.builder("order_id", BasicType.STRING_TYPE).build())
                        .build());

        assertPrepareFails(source, config("order_id"));
    }

    @Test
    public void rejectsMultipleSourceTables() {
        CatalogTable source = sourceTable(
                TableSchema.builder()
                        .column(Column.builder("id", BasicType.LONG_TYPE).build())
                        .build());
        Map<TablePath, CatalogTable> tables = new LinkedHashMap<TablePath, CatalogTable>();
        tables.put(source.getTablePath(), source);
        CatalogTable second = CatalogTable.builder(
                TablePath.of("source", "orders_2"),
                source.getTableSchema()).build();
        tables.put(second.getTablePath(), second);

        try {
            new MongoSinkPreparer(config(null))
                    .prepare(new SinkPrepareContext(ReadonlyConfig.fromMap(new LinkedHashMap<String, Object>()), tables));
            fail("Expected multi-table rejection");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    private static void assertPrepareFails(
            CatalogTable source,
            MongoSinkConfig config) {
        try {
            new MongoSinkPreparer(config).prepare(context(source));
            fail("Expected MongoDB Sink preparation failure");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    private static SinkPrepareContext context(CatalogTable source) {
        Map<TablePath, CatalogTable> tables = new LinkedHashMap<TablePath, CatalogTable>();
        tables.put(source.getTablePath(), source);
        return new SinkPrepareContext(
                ReadonlyConfig.fromMap(new LinkedHashMap<String, Object>()),
                tables);
    }

    private static CatalogTable sourceTable(TableSchema schema) {
        return CatalogTable.builder(TablePath.of("source", "orders"), schema).build();
    }

    private static MongoSinkConfig config(String documentIdField) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("uri", "mongodb://localhost:27017/archive");
        values.put("collection", "orders_copy");
        if (documentIdField != null) {
            values.put("document_id_field", documentIdField);
        }
        return MongoSinkConfig.of(ReadonlyConfig.fromMap(values));
    }
}
