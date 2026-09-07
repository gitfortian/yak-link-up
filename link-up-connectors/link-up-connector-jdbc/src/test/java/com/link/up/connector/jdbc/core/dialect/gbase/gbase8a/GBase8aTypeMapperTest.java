package com.link.up.connector.jdbc.core.dialect.gbase.gbase8a;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.SqlType;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class GBase8aTypeMapperTest {

    private final GBase8aTypeMapper mapper = new GBase8aTypeMapper();

    @Test
    public void mapsCommonGBase8aReadTypesWithoutMySqlTimezoneSemantics() throws Exception {
        assertType(SqlType.INT, "TINYINT", Types.TINYINT, 3, 0);
        assertType(SqlType.INT, "MEDIUMINT", Types.INTEGER, 8, 0);
        assertType(SqlType.BIGINT, "BIGINT", Types.BIGINT, 19, 0);
        assertType(SqlType.FLOAT, "FLOAT", Types.FLOAT, 12, 4);
        assertType(SqlType.DOUBLE, "DOUBLE", Types.DOUBLE, 22, 8);
        assertType(SqlType.DECIMAL, "DECIMAL", Types.DECIMAL, 20, 4);
        assertType(SqlType.DATE, "DATE", Types.DATE, 10, 0);
        assertType(SqlType.TIME, "TIME", Types.TIME, 8, 0);
        assertType(SqlType.TIMESTAMP, "DATETIME", Types.TIMESTAMP, 26, 6);
        assertType(SqlType.TIMESTAMP, "TIMESTAMP", Types.TIMESTAMP, 26, 6);
        assertType(SqlType.BYTES, "BLOB", Types.BLOB, 65535, 0);
        assertType(SqlType.STRING, "CLOB", Types.CLOB, 65535, 0);
    }

    @Test
    public void oversizedDecimalFallsBackToStringInsteadOfLosingPrecision() throws Exception {
        assertType(SqlType.STRING, "DECIMAL", Types.DECIMAL, 65, 30);
    }

    private void assertType(
            SqlType expected,
            String sourceType,
            int jdbcType,
            int precision,
            int scale) throws Exception {
        Column column = mapper.map(metadata(sourceType, jdbcType, precision, scale), 1);
        assertEquals(expected, column.getDataType().getSqlType());
    }

    private static ResultSetMetaData metadata(
            String sourceType,
            int jdbcType,
            int precision,
            int scale) {
        return (ResultSetMetaData) Proxy.newProxyInstance(
                GBase8aTypeMapperTest.class.getClassLoader(),
                new Class<?>[]{ResultSetMetaData.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("getColumnLabel".equals(name) || "getColumnName".equals(name)) {
                        return "value";
                    }
                    if ("getColumnTypeName".equals(name)) {
                        return sourceType;
                    }
                    if ("getColumnType".equals(name)) {
                        return jdbcType;
                    }
                    if ("getPrecision".equals(name)) {
                        return precision;
                    }
                    if ("getScale".equals(name)) {
                        return scale;
                    }
                    if ("isNullable".equals(name)) {
                        return ResultSetMetaData.columnNullable;
                    }
                    Class<?> type = method.getReturnType();
                    if (type == boolean.class) {
                        return false;
                    }
                    if (type == int.class) {
                        return 0;
                    }
                    if (type == long.class) {
                        return 0L;
                    }
                    return null;
                });
    }
}
