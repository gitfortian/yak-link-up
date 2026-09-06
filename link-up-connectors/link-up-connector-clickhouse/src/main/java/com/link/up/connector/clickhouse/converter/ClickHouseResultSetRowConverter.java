package com.link.up.connector.clickhouse.converter;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.SqlType;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

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
            case DECIMAL:
                return resultSet.getBigDecimal(index);
            case STRING:
                return resultSet.getString(index);
            case BYTES:
                return resultSet.getBytes(index);
            case DATE: {
                Date value = resultSet.getDate(index);
                return value == null ? null : value.toLocalDate();
            }
            case TIME: {
                Time value = resultSet.getTime(index);
                return value == null ? null : value.toLocalTime();
            }
            case TIMESTAMP: {
                Timestamp value = resultSet.getTimestamp(index);
                return value == null ? null : value.toLocalDateTime();
            }
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
}
