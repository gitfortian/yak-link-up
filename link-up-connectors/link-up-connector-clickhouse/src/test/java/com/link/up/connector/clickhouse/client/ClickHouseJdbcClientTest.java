package com.link.up.connector.clickhouse.client;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClickHouseJdbcClientTest {

    @Test
    public void buildsHttpJdbcUrl() {
        assertEquals(
                "jdbc:clickhouse:http://localhost:8123/analytics",
                ClickHouseJdbcClient.buildJdbcUrl("localhost:8123", "analytics"));
    }

    @Test
    public void preservesHttpsEndpoint() {
        assertEquals(
                "jdbc:clickhouse:https://cluster.example.com:8443/default",
                ClickHouseJdbcClient.buildJdbcUrl(
                        "https://cluster.example.com:8443/",
                        null));
    }

    @Test
    public void stripsTrailingSemicolons() {
        assertEquals(
                "select 1",
                ClickHouseJdbcClient.stripTrailingSemicolon(" select 1;;; "));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsJdbcUrlInsideHostOption() {
        ClickHouseJdbcClient.buildJdbcUrl(
                "jdbc:clickhouse:http://localhost:8123",
                "default");
    }
}
