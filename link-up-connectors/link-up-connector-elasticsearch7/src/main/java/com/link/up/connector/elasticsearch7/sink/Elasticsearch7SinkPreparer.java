package com.link.up.connector.elasticsearch7.sink;

import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPrepareContext;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.elasticsearch7.client.Elasticsearch7SinkClient;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SinkConfig;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Validates one existing Elasticsearch 7 target before bounded task writers start. */
final class Elasticsearch7SinkPreparer implements SinkPreparer {

    private final Elasticsearch7SinkConfig config;

    Elasticsearch7SinkPreparer(Elasticsearch7SinkConfig config) {
        this.config = config;
    }

    @Override
    public PreparedSinkMetadata prepare(SinkPrepareContext context) throws Exception {
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }
        if (context.getSourceTables().size() != 1) {
            throw new IllegalArgumentException(
                    "Elasticsearch 7 bounded Sink Stage 2 supports exactly one source table");
        }

        Map.Entry<TablePath, CatalogTable> sourceEntry =
                context.getSourceTables().entrySet().iterator().next();
        CatalogTable sourceTable = sourceEntry.getValue();
        if (sourceTable == null || sourceTable.getTableSchema() == null) {
            throw new IllegalArgumentException("Elasticsearch 7 Sink requires source table schema");
        }

        List<String> sourceFields = new ArrayList<String>();
        for (Column column : sourceTable.getTableSchema().getColumns()) {
            sourceFields.add(column.getName());
        }

        CatalogTable targetTable;
        try (Elasticsearch7SinkClient client = new Elasticsearch7SinkClient(config)) {
            targetTable = client.discoverTargetTable(sourceFields);
        }
        CatalogTable preparedTarget =
                validateAndReorder(sourceTable, targetTable, config.getDocumentIdField());

        Map<TablePath, CatalogTable> targets =
                new LinkedHashMap<TablePath, CatalogTable>();
        targets.put(sourceEntry.getKey(), preparedTarget);
        return new PreparedSinkMetadata(targets);
    }

    static CatalogTable validateAndReorder(
            CatalogTable sourceTable,
            CatalogTable targetTable,
            String documentIdField) {
        TableSchema source = sourceTable.getTableSchema();
        TableSchema target = targetTable.getTableSchema();
        if (documentIdField != null && !source.contains(documentIdField)) {
            throw new IllegalArgumentException(
                    "document_id_field does not exist in source schema: " + documentIdField);
        }

        TableSchema.Builder reordered = TableSchema.builder();
        for (Column sourceColumn : source.getColumns()) {
            if (!target.contains(sourceColumn.getName())) {
                throw new IllegalArgumentException(
                        "Elasticsearch target mapping is missing source field: " + sourceColumn.getName());
            }
            Column targetColumn = target.getColumn(sourceColumn.getName());
            validateColumn(sourceColumn, targetColumn);
            reordered.column(targetColumn);
        }
        return CatalogTable.builder(targetTable.getTablePath(), reordered.build())
                .options(targetTable.getOptions())
                .build();
    }

    private static void validateColumn(Column source, Column target) {
        SqlType sourceType = source.getDataType().getSqlType();
        String targetType =
                target.getSourceType() == null
                        ? "unknown"
                        : target.getSourceType().toLowerCase(Locale.ROOT);

        if ("boolean".equals(targetType)) {
            require(source, sourceType == SqlType.BOOLEAN, targetType);
            return;
        }
        if (isIntegerTarget(targetType)) {
            require(source, isSafeIntegerSource(sourceType, targetType), targetType);
            return;
        }
        if ("unsigned_long".equals(targetType)) {
            boolean supported = isInteger(sourceType);
            if (sourceType == SqlType.DECIMAL && source.getDataType() instanceof DecimalType) {
                DecimalType decimal = (DecimalType) source.getDataType();
                supported = decimal.getScale() == 0 && decimal.getPrecision() <= 20;
            }
            require(source, supported, targetType);
            return;
        }
        if ("half_float".equals(targetType) || "float".equals(targetType)) {
            require(source, sourceType == SqlType.FLOAT, targetType);
            return;
        }
        if ("double".equals(targetType)
                || "scaled_float".equals(targetType)
                || "rank_feature".equals(targetType)) {
            require(source, sourceType == SqlType.FLOAT || sourceType == SqlType.DOUBLE, targetType);
            return;
        }
        if (isTextTarget(targetType)) {
            require(source, sourceType == SqlType.STRING, targetType);
            return;
        }
        if ("date".equals(targetType) || "date_nanos".equals(targetType)) {
            require(
                    source,
                    sourceType == SqlType.STRING
                            || sourceType == SqlType.DATE
                            || sourceType == SqlType.TIMESTAMP
                            || sourceType == SqlType.TIMESTAMP_TZ,
                    targetType);
            return;
        }
        if ("binary".equals(targetType)) {
            require(source, sourceType == SqlType.BYTES || sourceType == SqlType.STRING, targetType);
            return;
        }
        if (isStructuredTarget(targetType)) {
            require(
                    source,
                    sourceType == SqlType.STRING
                            || sourceType == SqlType.ROW
                            || sourceType == SqlType.ARRAY,
                    targetType);
            return;
        }

        // Unknown/plugin mapping types stay intentionally conservative in Stage 2.
        require(source, sourceType == SqlType.STRING, targetType);
    }

    private static void require(Column source, boolean condition, String targetType) {
        if (!condition) {
            throw new IllegalArgumentException(
                    "Elasticsearch 7 target mapping is not a safe bounded-sink conversion for field "
                            + source.getName()
                            + ": source="
                            + source.getDataType().getSqlType()
                            + ", target="
                            + targetType);
        }
    }

    private static boolean isSafeIntegerSource(SqlType sourceType, String targetType) {
        if (!isInteger(sourceType)) {
            return false;
        }
        return integerRank(sourceType) <= integerRank(targetType);
    }

    private static boolean isInteger(SqlType type) {
        return type == SqlType.TINYINT
                || type == SqlType.SMALLINT
                || type == SqlType.INT
                || type == SqlType.BIGINT;
    }

    private static boolean isIntegerTarget(String targetType) {
        return "byte".equals(targetType)
                || "short".equals(targetType)
                || "integer".equals(targetType)
                || "token_count".equals(targetType)
                || "long".equals(targetType);
    }

    private static int integerRank(SqlType type) {
        switch (type) {
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INT:
                return 3;
            case BIGINT:
                return 4;
            default:
                return -1;
        }
    }

    private static int integerRank(String targetType) {
        if ("byte".equals(targetType)) {
            return 1;
        }
        if ("short".equals(targetType)) {
            return 2;
        }
        if ("integer".equals(targetType) || "token_count".equals(targetType)) {
            return 3;
        }
        if ("long".equals(targetType)) {
            return 4;
        }
        return -1;
    }

    private static boolean isTextTarget(String targetType) {
        return "text".equals(targetType)
                || "keyword".equals(targetType)
                || "constant_keyword".equals(targetType)
                || "wildcard".equals(targetType)
                || "ip".equals(targetType)
                || "version".equals(targetType)
                || "search_as_you_type".equals(targetType)
                || "match_only_text".equals(targetType);
    }

    private static boolean isStructuredTarget(String targetType) {
        return "object".equals(targetType)
                || "nested".equals(targetType)
                || "flattened".equals(targetType)
                || "geo_point".equals(targetType)
                || "geo_shape".equals(targetType)
                || "shape".equals(targetType)
                || "point".equals(targetType)
                || "dense_vector".equals(targetType)
                || "sparse_vector".equals(targetType)
                || "histogram".equals(targetType)
                || "rank_features".equals(targetType)
                || "completion".equals(targetType)
                || "join".equals(targetType)
                || "percolator".equals(targetType)
                || targetType.endsWith("_range");
    }
}
