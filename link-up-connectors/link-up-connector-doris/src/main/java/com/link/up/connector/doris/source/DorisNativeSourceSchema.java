package com.link.up.connector.doris.source;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.doris.config.DorisSourceTableConfig;

import java.util.List;
import java.util.Locale;

/** Applies projection and conservative native-scan type boundaries to discovered Doris metadata. */
final class DorisNativeSourceSchema {

    private DorisNativeSourceSchema() {
    }

    static CatalogTable prepare(CatalogTable discovered, DorisSourceTableConfig tableConfig) {
        if (discovered == null || discovered.getTableSchema() == null) {
            throw new IllegalArgumentException("Doris Catalog returned empty table metadata");
        }
        TableSchema schema = discovered.getTableSchema();
        List<String> projected = tableConfig.getReadFields();
        if (!projected.isEmpty()) {
            schema = schema.project(projected);
        }
        schema = normalizeScalarTypes(schema);
        validateNativeTypes(schema);
        return discovered.withSchema(schema)
                .toBuilder()
                .option("connector", "doris")
                .option("read_mode", "native-thrift-arrow")
                .build();
    }

    private static TableSchema normalizeScalarTypes(TableSchema schema) {
        TableSchema.Builder builder = TableSchema.builder();
        for (Column column : schema.getColumns()) {
            String sourceType = column.getSourceType();
            String normalized =
                    sourceType == null ? "" : sourceType.toLowerCase(Locale.ROOT).trim();
            if (normalized.startsWith("largeint")) {
                // Doris LARGEINT is signed 128-bit; STRING preserves the complete range.
                builder.column(column.toBuilder()
                        .dataType(BasicType.STRING_TYPE)
                        .precision(null)
                        .scale(null)
                        .build());
            } else if (normalized.startsWith("tinyint")) {
                builder.column(column.toBuilder().dataType(BasicType.BYTE_TYPE).build());
            } else if (normalized.startsWith("smallint")) {
                builder.column(column.toBuilder().dataType(BasicType.SHORT_TYPE).build());
            } else {
                builder.column(column);
            }
        }
        if (schema.getPrimaryKey() != null) {
            builder.primaryKey(schema.getPrimaryKey());
        }
        return builder.build();
    }

    static void validateNativeTypes(TableSchema schema) {
        for (Column column : schema.getColumns()) {
            String sourceType = column.getSourceType();
            if (sourceType == null) {
                continue;
            }
            String normalized = sourceType.toLowerCase(Locale.ROOT).trim();
            if (normalized.startsWith("array")
                    || normalized.startsWith("map")
                    || normalized.startsWith("struct")
                    || normalized.startsWith("hll")
                    || normalized.startsWith("bitmap")) {
                throw new IllegalArgumentException(
                        "Doris native Source does not support complex/aggregate type in this stage: column="
                                + column.getName()
                                + ", type="
                                + sourceType);
            }
        }
    }
}
