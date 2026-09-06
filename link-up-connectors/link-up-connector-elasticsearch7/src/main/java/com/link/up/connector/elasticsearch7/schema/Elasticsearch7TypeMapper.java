package com.link.up.connector.elasticsearch7.schema;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Restrained Elasticsearch mapping-to-Link-Up schema conversion for bounded ES7 reads. */
public final class Elasticsearch7TypeMapper {

    private Elasticsearch7TypeMapper() {
    }

    public static TableSchema toTableSchema(
            Map<String, Object> mapping,
            List<String> projectedFields) {
        Map<String, Object> properties = mapValue(mapping == null ? null : mapping.get("properties"));
        if (properties.isEmpty()) {
            throw new IllegalArgumentException("Elasticsearch index mapping does not contain any properties");
        }

        List<String> fields =
                projectedFields == null || projectedFields.isEmpty()
                        ? new ArrayList<String>(properties.keySet())
                        : new ArrayList<String>(projectedFields);

        TableSchema.Builder builder = TableSchema.builder();
        for (String field : fields) {
            Map<String, Object> fieldMapping = findFieldMapping(properties, field);
            if (fieldMapping == null) {
                throw new IllegalArgumentException("Cannot find Elasticsearch mapping for source field: " + field);
            }
            String sourceType = stringValue(fieldMapping.get("type"));
            if (sourceType == null && fieldMapping.containsKey("properties")) {
                sourceType = "object";
            }
            if (sourceType == null) {
                sourceType = "unknown";
            }
            Column.Builder column =
                    Column.builder(field, toFluxType(sourceType))
                            .nullable(true)
                            .sourceType(sourceType);
            String format = stringValue(fieldMapping.get("format"));
            if (format != null) {
                column.attribute("format", format);
            }
            builder.column(column.build());
        }
        return builder.build();
    }

    private static FluxDataType<?> toFluxType(String sourceType) {
        String type = sourceType == null ? "" : sourceType;
        switch (type) {
            case "boolean":
                return BasicType.BOOLEAN_TYPE;
            case "byte":
                return BasicType.BYTE_TYPE;
            case "short":
                return BasicType.SHORT_TYPE;
            case "integer":
            case "token_count":
                return BasicType.INT_TYPE;
            case "long":
                return BasicType.LONG_TYPE;
            case "unsigned_long":
                return new DecimalType(20, 0);
            case "half_float":
            case "float":
                return BasicType.FLOAT_TYPE;
            case "double":
            case "scaled_float":
            case "rank_feature":
                return BasicType.DOUBLE_TYPE;
            default:
                // Stage 1 intentionally preserves date/object/nested/geo/vector/range values
                // as source text/JSON instead of inventing lossy typed semantics.
                return BasicType.STRING_TYPE;
        }
    }

    private static Map<String, Object> findFieldMapping(
            Map<String, Object> rootProperties,
            String fieldPath) {
        String[] parts = fieldPath.split("\\.");
        Map<String, Object> properties = rootProperties;
        Map<String, Object> current = null;
        for (int index = 0; index < parts.length; index++) {
            current = mapValue(properties.get(parts[index]));
            if (current.isEmpty()) {
                return null;
            }
            if (index < parts.length - 1) {
                properties = mapValue(current.get("properties"));
                if (properties.isEmpty()) {
                    return null;
                }
            }
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapValue(Object value) {
        if (!(value instanceof Map)) {
            return Collections.emptyMap();
        }
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            if (entry.getKey() != null) {
                result.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        return result;
    }

    private static String stringValue(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }
}
