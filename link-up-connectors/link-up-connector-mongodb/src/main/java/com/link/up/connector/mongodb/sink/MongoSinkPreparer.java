package com.link.up.connector.mongodb.sink;

import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPrepareContext;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.mongodb.config.MongoSinkConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Prepares one bounded MongoDB target collection. */
final class MongoSinkPreparer implements SinkPreparer {

    private final MongoSinkConfig config;

    MongoSinkPreparer(MongoSinkConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public PreparedSinkMetadata prepare(SinkPrepareContext context) {
        Objects.requireNonNull(context, "context must not be null");
        if (context.getSourceTables().size() != 1) {
            throw new IllegalArgumentException(
                    "MongoDB bounded Sink supports exactly one source table");
        }

        Map<TablePath, CatalogTable> targets = new LinkedHashMap<TablePath, CatalogTable>();
        for (Map.Entry<TablePath, CatalogTable> entry : context.getSourceTables().entrySet()) {
            CatalogTable sourceTable = Objects.requireNonNull(
                    entry.getValue(),
                    "source table must not be null");
            TableSchema schema = Objects.requireNonNull(
                    sourceTable.getTableSchema(),
                    "MongoDB Sink requires source table schema");

            validateSchema(schema);
            CatalogTable targetTable = CatalogTable.builder(config.getTargetPath(), schema)
                    .options(sourceTable.getOptions())
                    .option("mongodb.collection", config.getCollection())
                    .build();
            targets.put(entry.getKey(), targetTable);
        }
        return new PreparedSinkMetadata(targets);
    }

    private void validateSchema(TableSchema schema) {
        String documentIdField = config.getDocumentIdField();
        if (documentIdField != null && !schema.contains(documentIdField)) {
            throw new IllegalArgumentException(
                    "document_id_field does not exist in source schema: " + documentIdField);
        }
        if (documentIdField != null
                && !"_id".equals(documentIdField)
                && schema.contains("_id")) {
            throw new IllegalArgumentException(
                    "Source schema already contains _id; document_id_field would create ambiguous MongoDB identity");
        }

        for (Column column : schema.getColumns()) {
            validateType(column);
        }
        validatePathOverlaps(schema);
    }

    private static void validateType(Column column) {
        SqlType type = column.getDataType().getSqlType();
        switch (type) {
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BYTES:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
            case NULL:
                return;
            case ARRAY:
            case MAP:
            case ROW:
            default:
                throw new IllegalArgumentException(
                        "MongoDB bounded Sink does not support Link-Up type "
                                + type
                                + " for field "
                                + column.getName()
                                + "; MongoDB-origin complex values should use the Stage 1 STRING/Extended JSON boundary");
        }
    }

    private static void validatePathOverlaps(TableSchema schema) {
        List<Column> columns = new ArrayList<Column>(schema.getColumns());
        Collections.sort(
                columns,
                new Comparator<Column>() {
                    @Override
                    public int compare(Column left, Column right) {
                        return left.getName().compareTo(right.getName());
                    }
                });

        for (int i = 0; i < columns.size(); i++) {
            Column parent = columns.get(i);
            String prefix = parent.getName() + ".";
            for (int j = i + 1; j < columns.size(); j++) {
                Column child = columns.get(j);
                if (!child.getName().startsWith(prefix)) {
                    if (child.getName().compareTo(prefix) > 0) {
                        break;
                    }
                    continue;
                }
                if (!isRestorableMongoDocument(parent)) {
                    throw new IllegalArgumentException(
                            "MongoDB target field paths overlap but the parent is not a MongoDB document: parent="
                                    + parent.getName()
                                    + ", child="
                                    + child.getName());
                }
            }
        }
    }

    private static boolean isRestorableMongoDocument(Column column) {
        Map<String, String> attributes = column.getAttributes();
        String encoding = attributes.get("mongodb.encoding");
        String heterogeneous = attributes.get("mongodb.heterogeneous");
        String bsonTypes = attributes.get("mongodb.bsonTypes");
        return "extended-json".equalsIgnoreCase(encoding)
                && !"true".equalsIgnoreCase(heterogeneous)
                && containsType(bsonTypes, "DOCUMENT");
    }

    private static boolean containsType(String values, String expected) {
        if (values == null) {
            return false;
        }
        for (String value : values.split(",")) {
            if (expected.equalsIgnoreCase(value.trim())) {
                return true;
            }
        }
        return false;
    }
}
