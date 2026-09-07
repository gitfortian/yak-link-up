package com.link.up.connector.jdbc.core.dialect.gbase.gbase8a;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

/**
 * GBase 8a bounded Source type mapper.
 *
 * <p>The mapper follows the JDBC types documented by GBase 8a and keeps the contract read-only.
 * Unknown/extension types fall back to STRING so schema discovery stays usable without inventing
 * GBase-specific runtime types.</p>
 */
public final class GBase8aTypeMapper implements JdbcTypeMapper {

    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final int DEFAULT_DECIMAL_PRECISION = 38;
    private static final int DEFAULT_DECIMAL_SCALE = 18;

    @Override
    public Column map(ResultSetMetaData metadata, int columnIndex) throws SQLException {
        String name = firstText(
                metadata.getColumnLabel(columnIndex),
                metadata.getColumnName(columnIndex));
        int jdbcType = metadata.getColumnType(columnIndex);
        String sourceType = metadata.getColumnTypeName(columnIndex);
        int precision = metadata.getPrecision(columnIndex);
        int scale = metadata.getScale(columnIndex);
        FluxDataType<?> type = mapType(jdbcType, sourceType, precision, scale);

        Column.Builder builder = Column.builder(name, type)
                .nullable(metadata.isNullable(columnIndex) != ResultSetMetaData.columnNoNulls)
                .sourceType(sourceType);
        applyProperties(builder, type.getSqlType(), precision, scale);
        return builder.build();
    }

    /** Maps one row returned by {@link DatabaseMetaData#getColumns}. */
    public Column toColumn(ResultSet row) throws SQLException {
        String name = row.getString("COLUMN_NAME");
        int jdbcType = row.getInt("DATA_TYPE");
        String sourceType = row.getString("TYPE_NAME");
        int precision = intValue(row, "COLUMN_SIZE");
        int scale = intValue(row, "DECIMAL_DIGITS");
        FluxDataType<?> type = mapType(jdbcType, sourceType, precision, scale);

        Column.Builder builder = Column.builder(name, type)
                .nullable(row.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls)
                .defaultValue(safeObject(row, "COLUMN_DEF"))
                .autoIncrement("YES".equalsIgnoreCase(safeString(row, "IS_AUTOINCREMENT")))
                .comment(safeString(row, "REMARKS"))
                .sourceType(sourceType);
        applyProperties(builder, type.getSqlType(), precision, scale);
        return builder.build();
    }

    @Override
    public String toDatabaseType(Column column) {
        throw new UnsupportedOperationException(
                "GBase 8a Stage 1 is source-only; target type generation belongs to the Sink stage");
    }

    private static FluxDataType<?> mapType(
            int jdbcType,
            String sourceType,
            int precision,
            int scale) {

        String type = normalizeType(sourceType);
        if ("BOOL".equals(type) || "BOOLEAN".equals(type)) {
            return BasicType.BOOLEAN_TYPE;
        }
        if ("TINYINT".equals(type)
                || "SMALLINT".equals(type)
                || "MEDIUMINT".equals(type)
                || "INT".equals(type)
                || "INTEGER".equals(type)
                || "YEAR".equals(type)) {
            return BasicType.INT_TYPE;
        }
        if ("BIGINT".equals(type)) {
            return BasicType.LONG_TYPE;
        }
        if ("FLOAT".equals(type)) {
            return BasicType.FLOAT_TYPE;
        }
        if ("REAL".equals(type) || "DOUBLE".equals(type)) {
            return BasicType.DOUBLE_TYPE;
        }
        if ("DECIMAL".equals(type) || "NUMERIC".equals(type)) {
            return decimal(precision, scale);
        }
        if ("DATE".equals(type)) {
            return BasicType.DATE_TYPE;
        }
        if ("TIME".equals(type)) {
            return BasicType.TIME_TYPE;
        }
        if ("DATETIME".equals(type) || "TIMESTAMP".equals(type)) {
            return BasicType.TIMESTAMP_TYPE;
        }
        if (isBinary(type)) {
            return BasicType.BYTES_TYPE;
        }
        if (isString(type)) {
            return BasicType.STRING_TYPE;
        }

        switch (jdbcType) {
            case Types.BOOLEAN:
            case Types.BIT:
                return BasicType.BOOLEAN_TYPE;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return BasicType.INT_TYPE;
            case Types.BIGINT:
                return BasicType.LONG_TYPE;
            case Types.FLOAT:
            case Types.REAL:
                return BasicType.FLOAT_TYPE;
            case Types.DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case Types.DECIMAL:
            case Types.NUMERIC:
                return decimal(precision, scale);
            case Types.DATE:
                return BasicType.DATE_TYPE;
            case Types.TIME:
                return BasicType.TIME_TYPE;
            case Types.TIMESTAMP:
                return BasicType.TIMESTAMP_TYPE;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return BasicType.TIMESTAMP_TZ_TYPE;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return BasicType.BYTES_TYPE;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.SQLXML:
            case Types.OTHER:
            default:
                return BasicType.STRING_TYPE;
        }
    }

    private static FluxDataType<?> decimal(int precision, int scale) {
        if (precision > MAX_DECIMAL_PRECISION) {
            return BasicType.STRING_TYPE;
        }
        int resolvedPrecision = precision > 0
                ? Math.max(1, precision)
                : DEFAULT_DECIMAL_PRECISION;
        int resolvedScale = scale >= 0
                ? Math.min(scale, resolvedPrecision)
                : DEFAULT_DECIMAL_SCALE;
        return new DecimalType(resolvedPrecision, resolvedScale);
    }

    private static void applyProperties(
            Column.Builder builder,
            SqlType type,
            int precision,
            int scale) {
        if ((type == SqlType.STRING || type == SqlType.BYTES) && precision > 0) {
            builder.length((long) precision);
            return;
        }
        if (type == SqlType.DECIMAL) {
            if (precision > 0 && precision <= MAX_DECIMAL_PRECISION) {
                builder.precision(precision);
            }
            if (scale >= 0 && precision > 0 && precision <= MAX_DECIMAL_PRECISION) {
                builder.scale(Math.min(scale, precision));
            }
            return;
        }
        if ((type == SqlType.TIME || type == SqlType.TIMESTAMP) && scale > 0) {
            builder.precision(scale);
        }
    }

    private static boolean isBinary(String type) {
        return "BINARY".equals(type)
                || "VARBINARY".equals(type)
                || "TINYBLOB".equals(type)
                || "BLOB".equals(type)
                || "MEDIUMBLOB".equals(type)
                || "LONGBLOB".equals(type);
    }

    private static boolean isString(String type) {
        return "CHAR".equals(type)
                || "VARCHAR".equals(type)
                || "TINYTEXT".equals(type)
                || "TEXT".equals(type)
                || "MEDIUMTEXT".equals(type)
                || "LONGTEXT".equals(type)
                || "CLOB".equals(type)
                || "ENUM".equals(type)
                || "SET".equals(type)
                || "JSON".equals(type);
    }

    private static String normalizeType(String value) {
        if (value == null) {
            return "";
        }
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        int parenthesis = normalized.indexOf('(');
        if (parenthesis > 0) {
            normalized = normalized.substring(0, parenthesis).trim();
        }
        int space = normalized.indexOf(' ');
        if (space > 0) {
            normalized = normalized.substring(0, space).trim();
        }
        return normalized;
    }

    private static String firstText(String first, String second) {
        if (first != null && !first.trim().isEmpty()) {
            return first.trim();
        }
        if (second != null && !second.trim().isEmpty()) {
            return second.trim();
        }
        throw new IllegalArgumentException("column name must not be empty");
    }

    private static int intValue(ResultSet row, String column) throws SQLException {
        Object value = row.getObject(column);
        return value == null ? 0 : ((Number) value).intValue();
    }

    private static String safeString(ResultSet row, String column) {
        try {
            return row.getString(column);
        } catch (SQLException ignored) {
            return null;
        }
    }

    private static Object safeObject(ResultSet row, String column) {
        try {
            return row.getObject(column);
        } catch (SQLException ignored) {
            return null;
        }
    }
}
