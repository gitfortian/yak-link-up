package com.link.up.connector.jdbc.core.dialect.goldendb;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.config.ReadConsistency;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectLoader;
import com.link.up.connector.jdbc.core.dialect.mysql.MySqlTypeMapper;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GoldenDbDialectTest {

    @Test
    public void explicitDialectLoadsGoldenDbForMysqlJdbcUrl() {
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        config(
                                "jdbc:mysql://127.0.0.1:3306/test",
                                "goldendb"));

        assertTrue(dialect instanceof GoldenDbDialect);
        assertEquals(
                DatabaseIdentifier.GOLDENDB,
                dialect.name());
    }

    @Test
    public void factoryDoesNotCompeteWithMysqlUrlDetection() {
        assertFalse(
                new GoldenDbDialectFactory()
                        .acceptsUrl(
                                "jdbc:mysql://127.0.0.1:3306/test"));
    }

    @Test
    public void reusesMysqlOfflineTypeTableAndUpsertSemantics() {
        GoldenDbDialect dialect =
                new GoldenDbDialect(
                        config(
                                "jdbc:mysql://127.0.0.1:3306/test",
                                "goldendb"));

        assertTrue(
                dialect.typeMapper()
                        instanceof MySqlTypeMapper);
        assertEquals(
                "`test`.`orders`",
                dialect.tableIdentifier(
                        TablePath.of("orders")));

        String upsert =
                dialect.buildUpsertSql(
                                TablePath.of("test", "orders"),
                                Arrays.asList("id", "name"),
                                Arrays.asList("id"))
                        .get();

        assertTrue(
                upsert.contains(
                        "ON DUPLICATE KEY UPDATE"));
    }

    @Test
    public void rowConverterKeepsGoldenDbIdentity() {
        GoldenDbDialect dialect =
                new GoldenDbDialect(
                        config(
                                "jdbc:mysql://127.0.0.1:3306/test",
                                "goldendb"));

        assertEquals(
                DatabaseIdentifier.GOLDENDB,
                dialect.rowConverter().name());
    }

    @Test
    public void stageOneDoesNotAdvertiseCoordinatedDatabaseSnapshot() {
        GoldenDbDialect dialect =
                new GoldenDbDialect(
                        config(
                                "jdbc:mysql://127.0.0.1:3306/test",
                                "goldendb"));

        assertTrue(
                dialect.supportedReadConsistencies()
                        .contains(ReadConsistency.BEST_EFFORT));
        assertTrue(
                dialect.supportedReadConsistencies()
                        .contains(
                                ReadConsistency.SINGLE_CONNECTION_SNAPSHOT));
        assertFalse(
                dialect.supportedReadConsistencies()
                        .contains(
                                ReadConsistency.DATABASE_SNAPSHOT));
    }

    private static JdbcConnectionConfig config(
            String url,
            String dialect) {

        Map<String, Object> values =
                new LinkedHashMap<String, Object>();

        values.put("url", url);
        values.put(
                "driver",
                "com.mysql.cj.jdbc.Driver");
        values.put("dialect", dialect);

        return JdbcConnectionConfig.of(
                ReadonlyConfig.fromMap(values));
    }
}
