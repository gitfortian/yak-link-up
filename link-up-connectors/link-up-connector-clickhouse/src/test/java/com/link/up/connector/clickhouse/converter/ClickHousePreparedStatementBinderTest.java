package com.link.up.connector.clickhouse.converter;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxRow;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ClickHousePreparedStatementBinderTest {

    @Test
    public void preservesDecimalTextAndLocalDateTime() throws Exception {
        TableSchema schema =
                TableSchema.builder()
                        .column(Column.builder("id", BasicType.LONG_TYPE).build())
                        .column(Column.builder("u64", new DecimalType(20, 0)).build())
                        .column(Column.builder("ts", BasicType.TIMESTAMP_TYPE).build())
                        .column(Column.builder("name", BasicType.STRING_TYPE).build())
                        .build();
        ClickHousePreparedStatementBinder binder = new ClickHousePreparedStatementBinder(schema);
        Map<String, Object[]> calls = new LinkedHashMap<String, Object[]>();
        PreparedStatement statement = recordingStatement(calls);
        BigDecimal maxUInt64 = new BigDecimal("18446744073709551615");
        LocalDateTime timestamp = LocalDateTime.of(2026, 9, 6, 12, 34, 56, 123456000);

        binder.bind(statement, FluxRow.of(7L, maxUInt64, timestamp, "order"));

        assertEquals(7L, ((Long) calls.get("setLong")[1]).longValue());
        assertEquals(maxUInt64, calls.get("setBigDecimal")[1]);
        assertSame(timestamp, calls.get("setObject")[1]);
        assertEquals("order", calls.get("setString")[1]);
    }

    private static PreparedStatement recordingStatement(final Map<String, Object[]> calls) {
        return (PreparedStatement)
                Proxy.newProxyInstance(
                        ClickHousePreparedStatementBinderTest.class.getClassLoader(),
                        new Class<?>[] {PreparedStatement.class},
                        (proxy, method, args) -> {
                            if (method.getName().startsWith("set")) {
                                calls.put(method.getName(), args);
                            }
                            Class<?> returnType = method.getReturnType();
                            if (returnType == boolean.class) {
                                return false;
                            }
                            if (returnType == int.class) {
                                return 0;
                            }
                            if (returnType == long.class) {
                                return 0L;
                            }
                            return null;
                        });
    }
}
