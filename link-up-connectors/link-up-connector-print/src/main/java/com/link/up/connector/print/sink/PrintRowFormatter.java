package com.link.up.connector.print.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.SqlType;
import com.link.up.api.table.type.FluxRow;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Pure line formatter for the Print Sink.
 *
 * <p>Being side-effect free, it is unit tested directly on strings without
 * any log-capture facility. The JSON payload keeps declared column names in
 * schema order; the declared Flux type decides the JSON representation so
 * output stays deterministic and machine-comparable.
 */
public final class PrintRowFormatter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP_TZ_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");

    public String formatSchema(String dataSetId, CatalogTable table) {
        Objects.requireNonNull(dataSetId, "dataSetId must not be null");
        Objects.requireNonNull(table, "table must not be null");

        StringBuilder line = new StringBuilder("schema: dataset=").append(dataSetId).append(" columns=[");
        TableSchema schema = table.getTableSchema();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            if (i > 0) {
                line.append(", ");
            }
            Column column = schema.getColumn(i);
            line.append(column.getName()).append('<').append(typeLabel(column)).append('>');
        }
        return line.append(']').toString();
    }

    public String formatRow(
            String dataSetId,
            String splitId,
            long rowNumber,
            CatalogTable table,
            FluxRow row) {

        Objects.requireNonNull(dataSetId, "dataSetId must not be null");
        Objects.requireNonNull(splitId, "splitId must not be null");
        Objects.requireNonNull(table, "table must not be null");
        Objects.requireNonNull(row, "row must not be null");

        TableSchema schema = table.getTableSchema();
        if (row.getArity() != schema.getColumnCount()) {
            throw new IllegalStateException(
                    "Row arity " + row.getArity() + " does not match the schema column count "
                            + schema.getColumnCount() + " for dataset " + dataSetId);
        }

        Map<String, Object> json = new LinkedHashMap<String, Object>();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            Column column = schema.getColumn(i);
            json.put(column.getName(), convertValue(column, row.getField(i)));
        }

        return "data: dataset=" + dataSetId
                + " split=" + splitId
                + " row=" + rowNumber
                + " " + toJson(json, dataSetId);
    }

    private String toJson(Map<String, Object> json, String dataSetId) {
        try {
            return MAPPER.writeValueAsString(json);
        } catch (Exception failure) {
            throw new IllegalStateException(
                    "Could not serialize Print Sink row for dataset " + dataSetId,
                    failure);
        }
    }

    private Object convertValue(Column column, Object value) {
        if (value == null) {
            return null;
        }

        SqlType type = column.getDataType().getSqlType();
        try {
            switch (type) {
                case STRING:
                    return (String) value;
                case BOOLEAN:
                    return (Boolean) value;
                case TINYINT:
                    return (Byte) value;
                case SMALLINT:
                    return (Short) value;
                case INT:
                    return (Integer) value;
                case BIGINT:
                    return (Long) value;
                case FLOAT:
                    return (Float) value;
                case DOUBLE:
                    return (Double) value;
                case DECIMAL:
                    return ((BigDecimal) value).toPlainString();
                case BYTES:
                    return Base64.getEncoder().encodeToString((byte[]) value);
                case DATE:
                    return ((LocalDate) value).format(DateTimeFormatter.ISO_LOCAL_DATE);
                case TIME:
                    return ((LocalTime) value).format(TIME_FORMAT);
                case TIMESTAMP:
                    return ((LocalDateTime) value).format(TIMESTAMP_FORMAT);
                case TIMESTAMP_TZ:
                    return ((OffsetDateTime) value).format(TIMESTAMP_TZ_FORMAT);
                default:
                    throw new IllegalStateException("unsupported type " + type);
            }
        } catch (RuntimeException failure) {
            throw new IllegalStateException(
                    "Column '" + column.getName() + "' carries a value that does not match its "
                            + "declared type " + type + ": " + value.getClass().getName(),
                    failure);
        }
    }

    private String typeLabel(Column column) {
        if (column.getDataType() instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) column.getDataType();
            return "DECIMAL(" + decimalType.getPrecision() + "," + decimalType.getScale() + ")";
        }
        return column.getDataType().getSqlType().name();
    }
}
