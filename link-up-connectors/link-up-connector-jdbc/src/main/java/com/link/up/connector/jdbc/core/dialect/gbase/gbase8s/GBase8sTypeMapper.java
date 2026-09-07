package com.link.up.connector.jdbc.core.dialect.gbase.gbase8s;

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
 * GBase 8s read-side and safe automatic-target type mapper.
 *
 * <p>The read path covers stable built-in scalar and large-object types. INTERVAL and unknown or
 * extension types keep the existing STRING boundary instead of leaking driver-specific Java
 * objects into Link-Up. Automatic target creation deliberately exposes only a conservative native
 * GBase 8s type subset and does not assume Oracle/MySQL SQLMODE compatibility.</p>
 */
public final class GBase8sTypeMapper implements JdbcTypeMapper {

    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final int DEFAULT_DECIMAL_PRECISION = 38;
    private static final int DEFAULT_DECIMAL_SCALE = 18;

    /** Conservative V8.8 target limits used by automatic table creation. */
    private static final int TARGET_MAX_DECIMAL_PRECISION = 32;
    private static final int TARGET_DEFAULT_DECIMAL_PRECISION = 32;
    private static final int TARGET_DEFAULT_DECIMAL_SCALE = 18;
    private static final long SAFE_VARCHAR_LENGTH = 8000L;

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
                .sourceType(sourceType)
                .autoIncrement(isSerial(sourceType));
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
                .autoIncrement(
                        isSerial(sourceType)
                                || "YES".equalsIgnoreCase(safeString(row, "IS_AUTOINCREMENT")))
                .comment(safeString(row, "REMARKS"))
                .sourceType(sourceType);
        applyProperties(builder, type.getSqlType(), precision, scale);
        return builder.build();
    }

    /**
     * Maps a Link-Up column to the portable native GBase 8s type used for automatic target creation.
     *
     * <p>Defaults, SERIAL/BIGSERIAL generation, comments and compatibility-mode type aliases are
     * intentionally not copied here.</p>
     */
    @Override
    public String toDatabaseType(Column column) {
        if (column == null || column.getDataType() == null) {
            throw new IllegalArgumentException("column/dataType must not be null");
        }

        SqlType type = column.getDataType().getSqlType();
        switch (type) {
            case STRING:
                return stringTargetType(column);
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INTEGER";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "SMALLFLOAT";
            case DOUBLE:
                return "FLOAT";
            case DECIMAL:
                return decimalTargetType(column);
            case BYTES:
                return "BYTE";
            case DATE:
                return "DATE";
            case TIME:
                return "DATETIME HOUR TO SECOND";
            case TIMESTAMP:
                return "DATETIME YEAR TO FRACTION(5)";
            case TIMESTAMP_TZ:
                throw unsupportedTargetType(
                        column,
                        "TIMESTAMP WITH TIME ZONE is compatibility-mode specific and is not enabled in the native GBase 8s stage");
            case ARRAY:
            case MAP:
            case ROW:
            case NULL:
            default:
                throw unsupportedTargetType(
                        column,
                        "automatic target creation only supports scalar relational types");
        }
    }

    private static String stringTargetType(Column column) {
        Long length = column.getLength();
        if (length != null && length > 0 && length <= SAFE_VARCHAR_LENGTH) {
            return "VARCHAR(" + length + ")";
        }
        return "TEXT";
    }

    private static String decimalTargetType(Column column) {
        int precision = TARGET_DEFAULT_DECIMAL_PRECISION;
        int scale = TARGET_DEFAULT_DECIMAL_SCALE;

        if (column.getDataType() instanceof DecimalType) {
            DecimalType decimal = (DecimalType) column.getDataType();
            precision = decimal.getPrecision();
            scale = decimal.getScale();
        } else {
            if (column.getPrecision() != null && column.getPrecision() > 0) {
                precision = column.getPrecision();
            }
            if (column.getScale() != null && column.getScale() >= 0) {
                scale = column.getScale();
            }
        }

        if (precision <= 0
                || precision > TARGET_MAX_DECIMAL_PRECISION
                || scale < 0
                || scale > precision) {
            throw unsupportedTargetType(
                    column,
                    "native GBase 8s automatic DECIMAL target requires precision <= 32 and scale <= precision");
        }
        return "DECIMAL(" + precision + "," + scale + ")";
    }

    private static UnsupportedOperationException unsupportedTargetType(
            Column column,
            String reason) {
        return new UnsupportedOperationException(
                "GBase 8s cannot auto-create target column '"
                        + column.getName()
                        + "' from Flux type "
                        + column.getDataType().getSqlType()
                        + ": "
                        + reason);
    }

    private static FluxDataType<?> mapType(
            int jdbcType,
            String sourceType,
            int precision,
            int scale) {
        String type = normalizeType(sourceType);

        if ("BOOLEAN".equals(type)) {
            return BasicType.BOOLEAN_TYPE;
        }
        if ("SMALLINT".equals(type)) {
            return BasicType.SHORT_TYPE;
        }
        if ("SERIAL".equals(type)
                || "INT".equals(type)
                || "INTEGER".equals(type)) {
            return BasicType.INT_TYPE;
        }
        if ("BIGINT".equals(type)
                || "INT8".equals(type)
                || "SERIAL8".equals(type)
                || "BIGSERIAL".equals(type)) {
            return BasicType.LONG_TYPE;
        }
        if ("SMALLFLOAT".equals(type) || "REAL".equals(type)) {
            return BasicType.FLOAT_TYPE;
        }
        if ("FLOAT".equals(type) || "DOUBLE".equals(type)) {
            return BasicType.DOUBLE_TYPE;
        }
        if ("DEC".equals(type)
                || "DECIMAL".equals(type)
                || "NUMERIC".equals(type)
                || "MONEY".equals(type)) {
            return decimal(precision, scale);
        }
        if ("DATE".equals(type)) {
            return BasicType.DATE_TYPE;
        }
        if ("DATETIME".equals(type)) {
            return BasicType.TIMESTAMP_TYPE;
        }
        if ("INTERVAL".equals(type)) {
            return BasicType.STRING_TYPE;
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
                return BasicType.SHORT_TYPE;
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

    private static boolean isSerial(String sourceType) {
        String type = normalizeType(sourceType);
        return "SERIAL".equals(type)
                || "SERIAL8".equals(type)
                || "BIGSERIAL".equals(type);
    }

    private static boolean isBinary(String type) {
        return "BYTE".equals(type)
                || "BLOB".equals(type)
                || "BINARY".equals(type)
                || "VARBINARY".equals(type);
    }

    private static boolean isString(String type) {
        return "CHAR".equals(type)
                || "CHARACTER".equals(type)
                || "VARCHAR".equals(type)
                || "LVARCHAR".equals(type)
                || "NCHAR".equals(type)
                || "NVARCHAR".equals(type)
                || "TEXT".equals(type)
                || "CLOB".equals(type);
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
        if (normalized.startsWith("DOUBLE PRECISION")) {
            return "DOUBLE";
        }
        if (normalized.startsWith("CHARACTER VARYING")) {
            return "VARCHAR";
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
