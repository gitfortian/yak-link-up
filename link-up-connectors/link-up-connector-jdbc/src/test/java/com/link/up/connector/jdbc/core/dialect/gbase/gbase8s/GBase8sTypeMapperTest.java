package com.link.up.connector.jdbc.core.dialect.gbase.gbase8s;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.SqlType;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GBase8sTypeMapperTest {

    private final GBase8sTypeMapper mapper = new GBase8sTypeMapper();

    @Test
    public void mapsNativeIntegerAndSerialFamilies() throws Exception {
        Column serial = map("SERIAL", Types.INTEGER, 10, 0);
        assertEquals(SqlType.INT, serial.getDataType().getSqlType());
        assertTrue(serial.isAutoIncrement());

        assertEquals(
                SqlType.BIGINT,
                map("INT8", Types.BIGINT, 19, 0).getDataType().getSqlType());
        assertEquals(
                SqlType.BIGINT,
                map("BIGSERIAL", Types.BIGINT, 19, 0).getDataType().getSqlType());
        assertEquals(
                SqlType.SMALLINT,
                map("SMALLINT", Types.SMALLINT, 5, 0).getDataType().getSqlType());
    }

    @Test
    public void mapsNativeFloatingDecimalAndMoneyFamilies() throws Exception {
        assertEquals(
                SqlType.FLOAT,
                map("SMALLFLOAT", Types.REAL, 8, 0).getDataType().getSqlType());
        assertEquals(
                SqlType.DOUBLE,
                map("FLOAT", Types.DOUBLE, 16, 0).getDataType().getSqlType());
        assertEquals(
                SqlType.DOUBLE,
                map("DOUBLE PRECISION", Types.DOUBLE, 16, 0).getDataType().getSqlType());
        assertEquals(
                SqlType.DECIMAL,
                map("MONEY(16,2)", Types.DECIMAL, 16, 2).getDataType().getSqlType());
    }

    @Test
    public void mapsDatetimeIntervalAndLargeObjectsConservatively() throws Exception {
        assertEquals(
                SqlType.TIMESTAMP,
                map("DATETIME YEAR TO SECOND", Types.TIMESTAMP, 0, 0)
                        .getDataType().getSqlType());
        assertEquals(
                SqlType.STRING,
                map("INTERVAL DAY TO SECOND", Types.OTHER, 0, 0)
                        .getDataType().getSqlType());
        assertEquals(
                SqlType.BYTES,
                map("BYTE", Types.LONGVARBINARY, 1024, 0)
                        .getDataType().getSqlType());
        assertEquals(
                SqlType.STRING,
                map("TEXT", Types.LONGVARCHAR, 1024, 0)
                        .getDataType().getSqlType());
    }

    @Test
    public void oversizedDecimalFallsBackToStringWithoutPrecisionLoss() throws Exception {
        Column column = map("DECIMAL(50,20)", Types.DECIMAL, 50, 20);
        assertEquals(SqlType.STRING, column.getDataType().getSqlType());
    }

    @Test
    public void unknownDriverExtensionFallsBackToString() throws Exception {
        assertEquals(
                SqlType.STRING,
                map("SOME_OPAQUE_TYPE", Types.OTHER, 0, 0)
                        .getDataType().getSqlType());
    }

    private Column map(
            String sourceType,
            int jdbcType,
            int precision,
            int scale) throws Exception {
        ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{ResultSetMetaData.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getColumnLabel":
                        case "getColumnName":
                            return "value";
                        case "getColumnType":
                            return jdbcType;
                        case "getColumnTypeName":
                            return sourceType;
                        case "getPrecision":
                            return precision;
                        case "getScale":
                            return scale;
                        case "isNullable":
                            return ResultSetMetaData.columnNullable;
                        default:
                            return defaultValue(method.getReturnType());
                    }
                });
        return mapper.map(metadata, 1);
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }
}
