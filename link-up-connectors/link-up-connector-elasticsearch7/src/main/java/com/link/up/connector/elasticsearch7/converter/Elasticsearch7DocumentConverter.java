package com.link.up.connector.elasticsearch7.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.ArrayType;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.FluxRowType;
import com.link.up.api.table.type.SqlType;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Converts one Link-Up row into one Elasticsearch index document. */
public final class Elasticsearch7DocumentConverter {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final java.math.BigDecimal UNSIGNED_LONG_MAX =
            new java.math.BigDecimal("18446744073709551615");

    private final TableSchema sourceSchema;
    private final TableSchema targetSchema;
    private final int documentIdIndex;

    public Elasticsearch7DocumentConverter(
            CatalogTable sourceTable,
            CatalogTable targetTable,
            String documentIdField) {
        Objects.requireNonNull(sourceTable, "sourceTable must not be null");
        Objects.requireNonNull(targetTable, "targetTable must not be null");
        this.sourceSchema = Objects.requireNonNull(sourceTable.getTableSchema(), "source schema must not be null");
        this.targetSchema = Objects.requireNonNull(targetTable.getTableSchema(), "target schema must not be null");
        if (sourceSchema.getColumnCount() != targetSchema.getColumnCount()) {
            throw new IllegalArgumentException("Prepared Elasticsearch target schema must match source field count");
        }
        this.documentIdIndex =
                documentIdField == null ? -1 : sourceSchema.indexOf(documentIdField);
        if (documentIdField != null && documentIdIndex < 0) {
            throw new IllegalArgumentException(
                    "document_id_field does not exist in source schema: " + documentIdField);
        }
    }

    public Document convert(FluxRow row) {
        Objects.requireNonNull(row, "row must not be null");
        sourceSchema.toRowType().validate(row);

        Map<String, Object> document = new LinkedHashMap<String, Object>();
        for (int index = 0; index < sourceSchema.getColumnCount(); index++) {
            Column sourceColumn = sourceSchema.getColumn(index);
            Column targetColumn = targetSchema.getColumn(index);
            if (!sourceColumn.getName().equals(targetColumn.getName())) {
                throw new IllegalStateException(
                        "Prepared Elasticsearch target schema order does not match source schema at column "
                                + sourceColumn.getName());
            }
            Object converted =
                    convertValue(
                            row.getField(index),
                            sourceColumn.getDataType(),
                            targetColumn.getSourceType(),
                            sourceColumn.getName());
            putDotted(document, sourceColumn.getName(), converted);
        }

        String documentId = null;
        if (documentIdIndex >= 0) {
            Object value = row.getField(documentIdIndex);
            if (value == null || String.valueOf(value).trim().isEmpty()) {
                throw new IllegalArgumentException("document_id_field value must not be null or blank");
            }
            documentId = String.valueOf(value);
            if (documentId.getBytes(StandardCharsets.UTF_8).length > 512) {
                throw new IllegalArgumentException("Elasticsearch document _id must not exceed 512 UTF-8 bytes");
            }
        }
        return new Document(documentId, document);
    }

    private static Object convertValue(
            Object value,
            FluxDataType<?> sourceType,
            String targetSourceType,
            String fieldName) {
        if (value == null) {
            return null;
        }
        String target = targetSourceType == null ? "" : targetSourceType.toLowerCase(Locale.ROOT);
        SqlType sqlType = sourceType.getSqlType();

        if ("unsigned_long".equals(target)) {
            java.math.BigDecimal decimal = toBigDecimal(value, fieldName);
            if (decimal.scale() > 0 || decimal.signum() < 0 || decimal.compareTo(UNSIGNED_LONG_MAX) > 0) {
                throw new IllegalArgumentException(
                        "Value is outside Elasticsearch unsigned_long range for field " + fieldName + ": " + value);
            }
            return decimal.toBigIntegerExact();
        }

        switch (sqlType) {
            case BYTES:
                if (!(value instanceof byte[])) {
                    throw new IllegalArgumentException("Expected byte[] for field " + fieldName);
                }
                return Base64.getEncoder().encodeToString((byte[]) value);
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
                return String.valueOf(value);
            case ROW:
                if (!(sourceType instanceof FluxRowType) || !(value instanceof FluxRow)) {
                    throw new IllegalArgumentException("Expected FluxRow value for field " + fieldName);
                }
                return rowToMap((FluxRowType) sourceType, (FluxRow) value, fieldName);
            case ARRAY:
                return arrayToList(value, sourceType, fieldName);
            case STRING:
                return convertString(String.valueOf(value), target, fieldName);
            default:
                return value;
        }
    }

    private static Object convertString(String value, String target, String fieldName) {
        if (isStructuredTarget(target)) {
            String trimmed = value.trim();
            if (!(trimmed.startsWith("{") || trimmed.startsWith("["))) {
                if ("geo_point".equals(target) || "completion".equals(target)) {
                    return value;
                }
                throw new IllegalArgumentException(
                        "Elasticsearch target type " + target + " requires JSON object/array text for field " + fieldName);
            }
            try {
                return OBJECT_MAPPER.readValue(trimmed, Object.class);
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException(
                        "Invalid JSON text for Elasticsearch field " + fieldName,
                        failure);
            }
        }
        return value;
    }

    private static boolean isStructuredTarget(String target) {
        return "object".equals(target)
                || "nested".equals(target)
                || "flattened".equals(target)
                || "geo_shape".equals(target)
                || "shape".equals(target)
                || "point".equals(target)
                || "dense_vector".equals(target)
                || "sparse_vector".equals(target)
                || "histogram".equals(target)
                || target.endsWith("_range")
                || "rank_features".equals(target);
    }

    private static Map<String, Object> rowToMap(
            FluxRowType rowType,
            FluxRow row,
            String fieldName) {
        rowType.validate(row);
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (int index = 0; index < rowType.getFieldCount(); index++) {
            result.put(
                    rowType.getFieldName(index),
                    convertValue(
                            row.getField(index),
                            rowType.getFieldType(index),
                            "object",
                            fieldName + "." + rowType.getFieldName(index)));
        }
        return result;
    }

    private static List<Object> arrayToList(
            Object value,
            FluxDataType<?> sourceType,
            String fieldName) {
        if (!(sourceType instanceof ArrayType)) {
            throw new IllegalArgumentException("Expected ArrayType for field " + fieldName);
        }
        if (!value.getClass().isArray()) {
            throw new IllegalArgumentException("Expected array value for field " + fieldName);
        }
        FluxDataType<?> elementType = ((ArrayType<?>) sourceType).getElementType();
        int length = Array.getLength(value);
        List<Object> result = new ArrayList<Object>(length);
        for (int index = 0; index < length; index++) {
            result.add(convertValue(Array.get(value, index), elementType, "object", fieldName));
        }
        return result;
    }

    private static java.math.BigDecimal toBigDecimal(Object value, String fieldName) {
        if (value instanceof java.math.BigDecimal) {
            return (java.math.BigDecimal) value;
        }
        if (value instanceof Number) {
            return new java.math.BigDecimal(String.valueOf(value));
        }
        try {
            return new java.math.BigDecimal(String.valueOf(value));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("Expected numeric value for field " + fieldName, failure);
        }
    }

    @SuppressWarnings("unchecked")
    private static void putDotted(Map<String, Object> root, String path, Object value) {
        String[] parts = path.split("\\.");
        Map<String, Object> current = root;
        for (int index = 0; index < parts.length - 1; index++) {
            Object existing = current.get(parts[index]);
            if (existing == null) {
                Map<String, Object> child = new LinkedHashMap<String, Object>();
                current.put(parts[index], child);
                current = child;
            } else if (existing instanceof Map) {
                current = (Map<String, Object>) existing;
            } else {
                throw new IllegalArgumentException(
                        "Conflicting Elasticsearch dotted field path: " + path);
            }
        }
        Object old = current.put(parts[parts.length - 1], value);
        if (old != null) {
            throw new IllegalArgumentException("Duplicate Elasticsearch document field path: " + path);
        }
    }

    public static final class Document {
        private final String id;
        private final Map<String, Object> source;

        Document(String id, Map<String, Object> source) {
            this.id = id;
            this.source = source;
        }

        public String getId() {
            return id;
        }

        public Map<String, Object> getSource() {
            return source;
        }
    }
}
