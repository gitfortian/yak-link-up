package com.link.up.connector.clickhouse.converter;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.SqlType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;

/** Converts one ClickHouse JDBC ResultSet row into a schema-aligned FluxRow. */
public final class ClickHouseResultSetRowConverter {

    private final TableSchema schema;

    public ClickHouseResultSetRowConverter(TableSchema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("schema must not be null");
        }
        this.schema = schema;
    }

    public FluxRow read(ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            throw new IllegalArgumentException("resultSet must not be null");
        }
        FluxRow row = new FluxRow(schema.getColumnCount());
        for (int index = 0; index < schema.getColumnCount(); index++) {
            Column column = schema.getColumn(index);
            row.setField(index, readValue(resultSet, index + 1, column.getDataType().getSqlType()));
        }
        return row;
    }

    private static Object readValue(ResultSet resultSet, int index, SqlType type)
            throws SQLException {
        switch (type) {
            case BOOLEAN: {
                boolean value = resultSet.getBoolean(index);
                return resultSet.wasNull() ? null : value;
            }
            case TINYINT: {
                byte value = resultSet.getByte(index);
                return resultSet.wasNull() ? null : value;
            }
            case SMALLINT: {
                short value = resultSet.getShort(index);
                return resultSet.wasNull() ? null : value;
            }
            case INT: {
                int value = resultSet.getInt(index);
                return resultSet.wasNull() ? null : value;
            }
            case BIGINT: {
                long value = resultSet.getLong(index);
                return resultSet.wasNull() ? null : value;
            }
            case FLOAT: {
                float value = resultSet.getFloat(index);
                return resultSet.wasNull() ? null : value;
            }
            case DOUBLE: {
                double value = resultSet.getDouble(index);
                return resultSet.wasNull() ? null : value;
            }
            case DECIMAL: {
                // Reading through text avoids narrowing UInt64/large Decimal values in older JDBC paths.
                String value = resultSet.getString(index);
                return value == null ? null : new BigDecimal(value);
            }
            case STRING:
                return resultSet.getString(index);
            case BYTES:
                return resultSet.getBytes(index);
            case DATE:
                return dateValue(resultSet.getObject(index));
            case TIME:
                return timeValue(resultSet.getObject(index));
            case TIMESTAMP:
                // clickhouse-jdbc 0.3.2-patch11 exposes DateTime64 as LocalDateTime via getObject().
                // Prefer that JSR-310 value so we do not re-interpret it through java.sql.Timestamp.
                return timestampValue(resultSet.getObject(index));
            case NULL:
                return null;
            case TIMESTAMP_TZ:
            case ARRAY:
            case MAP:
            case ROW:
            default:
                throw new SQLException(
                        "Unsupported ClickHouse ResultSet conversion target: " + type);
        }
    }

    private static LocalDate dateValue(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        if (value instanceof Date) {
            return ((Date) value).toLocalDate();
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        try {
            return LocalDate.parse(String.valueOf(value));
        } catch (RuntimeException failure) {
            throw new SQLException("Cannot convert ClickHouse DATE value: " + value, failure);
        }
    }

    private static LocalTime timeValue(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalTime) {
            return (LocalTime) value;
        }
        if (value instanceof Time) {
            return ((Time) value).toLocalTime();
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalTime();
        }
        try {
            return LocalTime.parse(String.valueOf(value));
        } catch (RuntimeException failure) {
            throw new SQLException("Cannot convert ClickHouse TIME value: " + value, failure);
        }
    }

    private static LocalDateTime timestampValue(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toLocalDateTime();
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toLocalDateTime();
        }
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toLocalDateTime();
        }

        String text = String.valueOf(value).trim().replace(' ', 'T');
        try {
            return LocalDateTime.parse(text);
        } catch (DateTimeParseException localFailure) {
            try {
                return OffsetDateTime.parse(text).toLocalDateTime();
            } catch (DateTimeParseException offsetFailure) {
                offsetFailure.addSuppressed(localFailure);
                throw new SQLException(
                        "Cannot convert ClickHouse TIMESTAMP value: " + value,
                        offsetFailure);
            }
        }
    }
}
