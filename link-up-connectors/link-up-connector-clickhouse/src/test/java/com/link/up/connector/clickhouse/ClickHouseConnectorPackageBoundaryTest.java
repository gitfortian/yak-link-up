package com.link.up.connector.clickhouse;

import com.link.up.connector.clickhouse.converter.ClickHousePreparedStatementBinder;
import com.link.up.connector.clickhouse.converter.ClickHouseResultSetRowConverter;
import com.link.up.connector.clickhouse.sink.ClickHouseSinkFactory;
import com.link.up.connector.clickhouse.source.ClickHouseSourceFactory;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ClickHouseConnectorPackageBoundaryTest {

    @Test
    public void factoriesUseStableIdentifier() {
        assertEquals("clickhouse", new ClickHouseSourceFactory().factoryIdentifier());
        assertEquals("clickhouse", new ClickHouseSinkFactory().factoryIdentifier());
    }

    @Test
    public void rowConversionBelongsToConverterRole() {
        assertEquals(
                "com.link.up.connector.clickhouse.converter",
                ClickHouseResultSetRowConverter.class.getPackage().getName());
        assertEquals(
                "com.link.up.connector.clickhouse.converter",
                ClickHousePreparedStatementBinder.class.getPackage().getName());
    }

    @Test
    public void forbiddenGenericRootPackagesMustNotExist() {
        File root = new File("src/main/java/com/link/up/connector/clickhouse");
        String[] forbidden = {"common", "core", "helper", "misc", "utils"};
        for (String name : forbidden) {
            assertFalse(
                    "Forbidden ClickHouse connector root package exists: " + name,
                    new File(root, name).exists());
        }
    }
}
