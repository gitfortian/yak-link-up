package com.link.up.connector.clickhouse.converter;

import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.SqlType;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;

/** Binds one FluxRow to a ClickHouse JDBC prepared INSERT without lossy coercion. */
public final class ClickHousePreparedStatementBinder {

    private final TableSchema sourceSchema;

    public ClickHousePreparedStatementBinder(TableSchema sourceSchema) {
        this.sourceSchema = Objects.requireNonNull(sourceSchema, "sourceSchema must not be null");
    }

    public void bind(PreparedStatement statement, FluxRow row) throws Exception {
        Objects.requireNonNull(statement, "statement must not be null");
        Objects.requireNonNull(row, "row must not be null");
        if (row.getArity() != sourceSchema.getColumnCount()) {
            throw new IllegalArgumentException(
                    "ClickHouse sink row arity does not match source schema: row="
                            + row.getArity()
                            + ", schema="
                            + sourceSchema.getColumnCount());
        }

        for (int index = 0; index < sourceSchema.getColumnCount(); index++) {
            Object value = row.getField(index);
            int parameter = index + 1;
            if (value == null) {
                statement.setObject(parameter, null);
                continue;
            }
            bindValue(statement, parameter, value, sourceSchema.getColumn(index).getDataType());
        }
    }

    private static void bindValue(
            PreparedStatement statement,
            int parameter,
            Object value,
            FluxDataType<?> type)
            throws Exception {
        SqlType sqlType = type.getSqlType();
        switch (sqlType) {
            case BOOLEAN:
                statement.setBoolean(parameter, booleanValue(value));
                return;
            case TINYINT:
                statement.setByte(parameter, number(value).byteValue());
                return;
            case SMALLINT:
                statement.setShort(parameter, number(value).shortValue());
                return;
            case INT:
                statement.setInt(parameter, number(value).intValue());
                return;
            case BIGINT:
                statement.setLong(parameter, number(value).longValue());
                return;
            case FLOAT:
                statement.setFloat(parameter, number(value).floatValue());
                return;
            case DOUBLE:
                statement.setDouble(parameter, number(value).doubleValue());
                return;
            case DECIMAL:
                statement.setBigDecimal(parameter, decimalValue(value));
                return;
            case STRING:
                statement.setString(parameter, String.valueOf(value));
                return;
            case DATE:
                if (value instanceof LocalDate) {
                    statement.setObject(parameter, value);
                } else if (value instanceof java.sql.Date) {
                    statement.setObject(parameter, ((java.sql.Date) value).toLocalDate());
                } else {
                    statement.setObject(parameter, LocalDate.parse(String.valueOf(value)));
                }
                return;
            case TIMESTAMP:
                if (value instanceof LocalDateTime) {
                    statement.setObject(parameter, value);
                } else if (value instanceof Timestamp) {
                    statement.setObject(parameter, ((Timestamp) value).toLocalDateTime());
                } else {
                    statement.setObject(
                            parameter,
                            LocalDateTime.parse(String.valueOf(value).replace(' ', 'T')));
                }
                return;
            case BYTES:
                throw unsupported(sqlType, "binary sink semantics are not defined for this stage");
            case TIME:
                throw unsupported(sqlType, "ClickHouse does not have a standalone TIME column type");
            case TIMESTAMP_TZ:
                throw unsupported(sqlType, "timezone conversion must be explicit before the ClickHouse sink");
            case ARRAY:
            case MAP:
            case ROW:
                throw unsupported(sqlType, "complex values need dedicated ClickHouse conversion semantics");
            case NULL:
            default:
                throw unsupported(sqlType, "unsupported non-null value");
        }
    }

    private static Boolean booleanValue(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        return Boolean.valueOf(String.valueOf(value));
    }

    private static Number number(Object value) {
        if (value instanceof Number) {
            return (Number) value;
        }
        return new BigDecimal(String.valueOf(value));
    }

    private static BigDecimal decimalValue(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        return new BigDecimal(String.valueOf(value));
    }

    private static IllegalArgumentException unsupported(SqlType type, String detail) {
        return new IllegalArgumentException(
                "Unsupported ClickHouse Sink Flux type " + type + ": " + detail);
    }
}
