package com.link.up.connector.clickhouse.converter;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxRow;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class ClickHouseResultSetRowConverterTest {

    @Test
    public void preservesUInt64TextAndLocalDateTime() throws Exception {
        TableSchema schema =
                TableSchema.builder()
                        .column(Column.builder("u64", new DecimalType(20, 0)).build())
                        .column(Column.builder("created_at", BasicType.TIMESTAMP_TYPE).build())
                        .build();
        final LocalDateTime timestamp = LocalDateTime.of(2026, 9, 6, 8, 42, 12, 123000000);

        ResultSet resultSet =
                (ResultSet)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {ResultSet.class},
                                (proxy, method, args) -> {
                                    if ("getString".equals(method.getName())
                                            && Integer.valueOf(1).equals(args[0])) {
                                        return "18446744073709551615";
                                    }
                                    if ("getObject".equals(method.getName())
                                            && Integer.valueOf(2).equals(args[0])) {
                                        return timestamp;
                                    }
                                    if ("toString".equals(method.getName())) {
                                        return "ClickHouseResultSetStub";
                                    }
                                    throw new UnsupportedOperationException(method.getName());
                                });

        FluxRow row = new ClickHouseResultSetRowConverter(schema).read(resultSet);

        assertEquals(new BigDecimal("18446744073709551615"), row.getField(0));
        assertEquals(timestamp, row.getField(1));
    }
}
