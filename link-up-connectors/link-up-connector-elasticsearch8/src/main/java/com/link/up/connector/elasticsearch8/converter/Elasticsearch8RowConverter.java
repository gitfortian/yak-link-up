package com.link.up.connector.elasticsearch8.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.SqlType;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/** Converts one Elasticsearch 8 _source document into a Link-Up FluxRow. */
public final class Elasticsearch8RowConverter {

    private final TableSchema schema;
    private final ObjectMapper objectMapper;

    public Elasticsearch8RowConverter(TableSchema schema) {
        this(schema, new ObjectMapper());
    }

    Elasticsearch8RowConverter(TableSchema schema, ObjectMapper objectMapper) {
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    public FluxRow convert(Map<String, Object> source) {
        Objects.requireNonNull(source, "source must not be null");
        FluxRow row = new FluxRow(schema.getColumnCount());
        for (int index = 0; index < schema.getColumnCount(); index++) {
            Column column = schema.getColumn(index);
            Object value = resolveValue(source, column.getName());
            row.setField(index, convertValue(value, column));
        }
        return row;
    }

    private Object convertValue(Object value, Column column) {
        if (value == null) {
            return null;
        }
        SqlType target = column.getDataType().getSqlType();
        if (target != SqlType.STRING && isMultiValue(value)) {
            throw new IllegalArgumentException(
                    "Elasticsearch field '" + column.getName()
                            + "' is multi-valued but mappings do not declare array cardinality; "
                            + "Stage 3 only auto-converts multi-valued/object content to STRING/JSON fields");
        }
        switch (target) {
            case STRING:
                return toStringValue(value);
            case BOOLEAN:
                return toBoolean(value, column.getName());
            case TINYINT:
                return number(value, column.getName()).byteValue();
            case SMALLINT:
                return number(value, column.getName()).shortValue();
            case INT:
                return number(value, column.getName()).intValue();
            case BIGINT:
                return number(value, column.getName()).longValue();
            case FLOAT:
                return number(value, column.getName()).floatValue();
            case DOUBLE:
                return number(value, column.getName()).doubleValue();
            case DECIMAL:
                return value instanceof BigDecimal ? value : new BigDecimal(String.valueOf(value));
            default:
                throw new IllegalArgumentException(
                        "Unsupported Elasticsearch 8 Stage 3 target type for field '"
                                + column.getName() + "': " + target);
        }
    }

    private String toStringValue(Object value) {
        if (value instanceof CharSequence) {
            return value.toString();
        }
        if (value instanceof Map || value instanceof Collection || value.getClass().isArray()) {
            try {
                return objectMapper.writeValueAsString(value);
            } catch (JsonProcessingException failure) {
                throw new IllegalArgumentException("Failed to serialize Elasticsearch field as JSON", failure);
            }
        }
        return String.valueOf(value);
    }

    private static Boolean toBoolean(Object value, String field) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        String text = String.valueOf(value).trim();
        if ("true".equalsIgnoreCase(text)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(text)) {
            return Boolean.FALSE;
        }
        throw new IllegalArgumentException("Cannot convert Elasticsearch field '" + field + "' to boolean: " + value);
    }

    private static Number number(Object value, String field) {
        if (value instanceof Number) {
            return (Number) value;
        }
        try {
            return new BigDecimal(String.valueOf(value));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(
                    "Cannot convert Elasticsearch field '" + field + "' to number: " + value,
                    failure);
        }
    }

    private static boolean isMultiValue(Object value) {
        return value instanceof Collection || value.getClass().isArray();
    }

    @SuppressWarnings("unchecked")
    private static Object resolveValue(Map<String, Object> source, String fieldPath) {
        if (source.containsKey(fieldPath)) {
            return source.get(fieldPath);
        }
        Object current = source;
        for (String part : fieldPath.split("\\.")) {
            if (!(current instanceof Map)) {
                return null;
            }
            Map<String, Object> map = (Map<String, Object>) current;
            if (!map.containsKey(part)) {
                return null;
            }
            current = map.get(part);
        }
        return current;
    }
}
