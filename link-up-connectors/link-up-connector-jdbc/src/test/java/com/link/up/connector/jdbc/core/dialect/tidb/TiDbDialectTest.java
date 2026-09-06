package com.link.up.connector.jdbc.core.dialect.tidb;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.config.ReadConsistency;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectLoader;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TiDbDialectTest {

    @Test
    public void explicitDialectLoadsTiDbForMysqlJdbcUrl() {
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        config(
                                "jdbc:mysql://127.0.0.1:4000/test",
                                "tidb"));

        assertTrue(dialect instanceof TiDbDialect);
        assertEquals(
                DatabaseIdentifier.TIDB,
                dialect.name());
    }

    @Test
    public void factoryDoesNotCompeteWithMysqlUrlDetection() {
        assertFalse(
                new TiDbDialectFactory()
                        .acceptsUrl(
                                "jdbc:mysql://127.0.0.1:4000/test"));
    }

    @Test
    public void reusesMysqlOfflineTableAndUpsertSemantics() {
        TiDbDialect dialect =
                new TiDbDialect(
                        config(
                                "jdbc:mysql://127.0.0.1:4000/test",
                                "tidb"));

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
    public void stageOneDoesNotAdvertiseCoordinatedDatabaseSnapshot() {
        TiDbDialect dialect =
                new TiDbDialect(
                        config(
                                "jdbc:mysql://127.0.0.1:4000/test",
                                "tidb"));

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
