package com.link.up.connector.clickhouse.schema;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClickHouseTypeMapperTest {

    @Test
    public void preservesUnsignedIntegerRanges() {
        assertEquals(
                BasicType.LONG_TYPE,
                ClickHouseTypeMapper.parse("UInt32").getDataType());
        assertEquals(
                new DecimalType(20, 0),
                ClickHouseTypeMapper.parse("UInt64").getDataType());
        assertEquals(
                BasicType.STRING_TYPE,
                ClickHouseTypeMapper.parse("UInt128").getDataType());
        assertEquals(
                BasicType.STRING_TYPE,
                ClickHouseTypeMapper.parse("Int256").getDataType());
    }

    @Test
    public void unwrapsNullableAndLowCardinality() {
        ClickHouseTypeMapper.TypeInfo info =
                ClickHouseTypeMapper.parse("Nullable(LowCardinality(String))");

        assertEquals(BasicType.STRING_TYPE, info.getDataType());
        assertTrue(info.isNullable());
    }

    @Test
    public void parsesDateTime64Precision() {
        ClickHouseTypeMapper.TypeInfo info =
                ClickHouseTypeMapper.parse("DateTime64(6, 'UTC')");

        assertEquals(BasicType.TIMESTAMP_TYPE, info.getDataType());
        assertEquals(Integer.valueOf(6), info.getPrecision());
        assertFalse(info.isNullable());
    }

    @Test
    public void columnRetainsOriginalType() {
        Column column = ClickHouseTypeMapper.toColumn("amount", "Nullable(Decimal(18, 2))");

        assertEquals(new DecimalType(18, 2), column.getDataType());
        assertEquals("Nullable(Decimal(18, 2))", column.getSourceType());
        assertTrue(column.isNullable());
        assertEquals(Integer.valueOf(18), column.getPrecision());
        assertEquals(Integer.valueOf(2), column.getScale());
    }

    @Test(expected = IllegalArgumentException.class)
    public void complexArrayFailsFast() {
        ClickHouseTypeMapper.parse("Array(UInt64)");
    }

    @Test(expected = IllegalArgumentException.class)
    public void aggregateFunctionFailsFast() {
        ClickHouseTypeMapper.parse("AggregateFunction(sum, UInt64)");
    }
}
