package com.link.up.connector.clickhouse.sink;

import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPrepareContext;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.clickhouse.client.ClickHouseSinkJdbcClient;
import com.link.up.connector.clickhouse.config.ClickHouseSinkConfig;

import java.util.LinkedHashMap;
import java.util.Map;

/** Validates one bounded ClickHouse target before task writers start. */
final class ClickHouseSinkPreparer implements SinkPreparer {

    private final ClickHouseSinkConfig config;

    ClickHouseSinkPreparer(ClickHouseSinkConfig config) {
        this.config = config;
    }

    @Override
    public PreparedSinkMetadata prepare(SinkPrepareContext context) throws Exception {
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }
        if (context.getSourceTables().size() != 1) {
            throw new IllegalArgumentException(
                    "ClickHouse bounded Sink Stage 2 supports exactly one source table");
        }

        Map.Entry<TablePath, CatalogTable> sourceEntry =
                context.getSourceTables().entrySet().iterator().next();
        CatalogTable sourceTable = sourceEntry.getValue();
        if (sourceTable == null || sourceTable.getTableSchema() == null) {
            throw new IllegalArgumentException("ClickHouse Sink requires source table schema");
        }

        CatalogTable discoveredTarget = ClickHouseSinkJdbcClient.discoverTargetTable(config);
        CatalogTable preparedTarget = validateAndReorder(sourceTable, discoveredTarget);

        Map<TablePath, CatalogTable> targets =
                new LinkedHashMap<TablePath, CatalogTable>();
        targets.put(sourceEntry.getKey(), preparedTarget);
        return new PreparedSinkMetadata(targets);
    }

    static CatalogTable validateAndReorder(
            CatalogTable sourceTable,
            CatalogTable targetTable) {
        TableSchema source = sourceTable.getTableSchema();
        TableSchema target = targetTable.getTableSchema();
        if (source.getColumnCount() != target.getColumnCount()) {
            throw new IllegalArgumentException(
                    "ClickHouse Sink Stage 2 requires source and target to have the same column count: source="
                            + source.getColumnCount()
                            + ", target="
                            + target.getColumnCount());
        }

        TableSchema.Builder reordered = TableSchema.builder();
        for (Column sourceColumn : source.getColumns()) {
            if (!target.contains(sourceColumn.getName())) {
                throw new IllegalArgumentException(
                        "ClickHouse target is missing source column: " + sourceColumn.getName());
            }
            Column targetColumn = target.getColumn(sourceColumn.getName());
            validateColumn(sourceColumn, targetColumn);
            reordered.column(targetColumn);
        }
        if (target.getPrimaryKey() != null
                && source.getColumns().stream()
                        .map(Column::getName)
                        .collect(java.util.stream.Collectors.toList())
                        .containsAll(target.getPrimaryKey().getColumnNames())) {
            reordered.primaryKey(target.getPrimaryKey());
        }

        return CatalogTable.builder(targetTable.getTablePath(), reordered.build())
                .options(targetTable.getOptions())
                .build();
    }

    private static void validateColumn(Column source, Column target) {
        SqlType sourceType = source.getDataType().getSqlType();
        SqlType targetType = target.getDataType().getSqlType();
        validateSupportedSourceType(source.getName(), sourceType);

        if (source.isNullable() && !target.isNullable()) {
            throw new IllegalArgumentException(
                    "Nullable source column cannot be written to non-null ClickHouse target: "
                            + source.getName());
        }

        if (sourceType == targetType) {
            if (sourceType == SqlType.DECIMAL) {
                validateDecimal(source, target);
            }
            return;
        }

        if (isInteger(sourceType) && isInteger(targetType)) {
            if (integerRank(targetType) >= integerRank(sourceType)) {
                return;
            }
        }
        if (sourceType == SqlType.FLOAT && targetType == SqlType.DOUBLE) {
            return;
        }

        throw new IllegalArgumentException(
                "ClickHouse target type is not a safe bounded-sink mapping for column "
                        + source.getName()
                        + ": source="
                        + sourceType
                        + ", target="
                        + target.getSourceType());
    }

    private static void validateSupportedSourceType(String column, SqlType type) {
        switch (type) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case STRING:
            case DATE:
            case TIMESTAMP:
                return;
            case BYTES:
            case TIME:
            case TIMESTAMP_TZ:
            case ARRAY:
            case MAP:
            case ROW:
            case NULL:
            default:
                throw new IllegalArgumentException(
                        "ClickHouse Sink Stage 2 does not support source type "
                                + type
                                + " for column "
                                + column);
        }
    }

    private static void validateDecimal(Column source, Column target) {
        FluxDataType<?> sourceDataType = source.getDataType();
        FluxDataType<?> targetDataType = target.getDataType();
        if (!(sourceDataType instanceof DecimalType) || !(targetDataType instanceof DecimalType)) {
            return;
        }
        DecimalType sourceDecimal = (DecimalType) sourceDataType;
        DecimalType targetDecimal = (DecimalType) targetDataType;
        int sourceIntegerDigits = sourceDecimal.getPrecision() - sourceDecimal.getScale();
        int targetIntegerDigits = targetDecimal.getPrecision() - targetDecimal.getScale();
        if (targetDecimal.getScale() < sourceDecimal.getScale()
                || targetIntegerDigits < sourceIntegerDigits) {
            throw new IllegalArgumentException(
                    "ClickHouse target decimal cannot represent source decimal for column "
                            + source.getName()
                            + ": source="
                            + sourceDecimal
                            + ", target="
                            + targetDecimal);
        }
    }

    private static boolean isInteger(SqlType type) {
        return type == SqlType.TINYINT
                || type == SqlType.SMALLINT
                || type == SqlType.INT
                || type == SqlType.BIGINT;
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
}
