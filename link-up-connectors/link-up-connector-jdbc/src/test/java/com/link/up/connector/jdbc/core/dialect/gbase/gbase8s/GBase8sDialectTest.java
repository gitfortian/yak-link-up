package com.link.up.connector.jdbc.core.dialect.gbase.gbase8s;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.WritableCatalog;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.config.ReadConsistency;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectLoader;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GBase8sDialectTest {

    @Test
    public void loadsDedicatedGBase8sDialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(baseUrl(), null, null, null));
        assertEquals(DatabaseIdentifier.GBASE8S, dialect.name());
    }

    @Test
    public void factoryOnlyAcceptsDedicatedGBase8sProtocol() {
        GBase8sDialectFactory factory = new GBase8sDialectFactory();
        assertTrue(factory.acceptsUrl(baseUrl()));
        assertFalse(factory.acceptsUrl("jdbc:gbase://127.0.0.1:5258/app"));
        assertFalse(factory.acceptsUrl("jdbc:gbase8c://127.0.0.1:5432/app"));
        assertFalse(factory.acceptsUrl("jdbc:mysql://127.0.0.1:3306/app"));
    }

    @Test
    public void explicitDialectKeepsFirstClassIdentity() {
        JdbcDialect dialect = JdbcDialectLoader.load(
                config(baseUrl(), null, null, DatabaseIdentifier.GBASE8S));
        assertEquals(DatabaseIdentifier.GBASE8S, dialect.name());
    }

    @Test
    public void extractsDatabaseAndServerFromCanonicalUrl() {
        assertEquals("testdb", GBase8sJdbcUrl.databaseName(baseUrl()));
        assertEquals(
                "gbase01",
                GBase8sJdbcUrl.serverName(baseUrl(), Collections.<String, String>emptyMap()));

        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("gbasedbtserver", "property-server");
        assertEquals(
                "property-server",
                GBase8sJdbcUrl.serverName(baseUrl(), properties));
    }

    @Test
    public void usesDatabaseOwnerTablePathSemantics() {
        GBase8sDialect dialect = dialect(null, null);
        assertEquals(TablePath.of("orders"), dialect.parseTablePath("orders"));
        assertEquals(
                TablePath.of(null, "appowner", "orders"),
                dialect.parseTablePath("appowner.orders"));
        assertEquals(
                TablePath.of("testdb", "appowner", "orders"),
                dialect.parseTablePath("testdb.appowner.orders"));

        assertEquals(
                "gbasedbt.orders",
                dialect.tableIdentifier(TablePath.of("orders")));
        assertEquals(
                "appowner.orders",
                dialect.tableIdentifier(TablePath.of(null, "appowner", "orders")));
    }

    @Test
    public void configuredSchemaActsAsDefaultOwner() {
        assertEquals(
                "appowner.orders",
                dialect("appowner", null).tableIdentifier(TablePath.of("orders")));
    }

    @Test
    public void rejectsCrossDatabasePath() {
        GBase8sDialect dialect = dialect(null, null);
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> dialect.parseTablePath("archive.appowner.orders"));
        assertTrue(error.getMessage().contains("不支持跨 database"));

        assertThrows(
                IllegalArgumentException.class,
                () -> dialect.tableIdentifier(
                        TablePath.of("archive", "appowner", "orders")));
    }

    @Test
    public void defaultDelimidentRejectsQuotedOrSpecialIdentifiers() {
        GBase8sDialect dialect = dialect(null, null);
        assertThrows(
                IllegalArgumentException.class,
                () -> dialect.parseTablePath("\"MixedOwner\".\"Orders\""));
        assertThrows(
                IllegalArgumentException.class,
                () -> dialect.quoteIdentifier("order-items"));
    }

    @Test
    public void explicitDelimidentPreservesQuotedIdentifierCase() {
        String url = baseUrl() + ";DELIMIDENT=y";
        GBase8sDialect dialect = new GBase8sDialect(config(url, null, null, null));
        assertEquals(
                TablePath.of(null, "MixedOwner", "OrderItems"),
                dialect.parseTablePath("\"MixedOwner\".\"OrderItems\""));
        assertEquals(
                "\"MixedOwner\".\"OrderItems\"",
                dialect.tableIdentifier(TablePath.of(null, "MixedOwner", "OrderItems")));
    }

    @Test
    public void sourceCatalogIsReadOnlyAndSinkSpecificSemanticsStayDisabled() {
        GBase8sDialect dialect = dialect(null, null);
        Catalog catalog = dialect.createCatalog(config(baseUrl(), null, null, null));
        assertFalse(catalog instanceof WritableCatalog);
        assertFalse(dialect.buildUpsertSql(
                TablePath.of("testdb", "gbasedbt", "orders"),
                Arrays.asList("id", "name"),
                Collections.singletonList("id")).isPresent());

        Column column = Column.builder("name", BasicType.STRING_TYPE).build();
        assertThrows(
                UnsupportedOperationException.class,
                () -> dialect.typeMapper().toDatabaseType(column));
    }

    @Test
    public void boundedSourceAdvertisesBestEffortOnlyAndKeepsIdentity() {
        assertEquals(
                Collections.singleton(ReadConsistency.BEST_EFFORT),
                dialect(null, null).supportedReadConsistencies());
        assertEquals(
                DatabaseIdentifier.GBASE8S,
                dialect(null, null).rowConverter().name());
    }

    @Test
    public void requiresDatabaseAndServerIdentity() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GBase8sDialect(config(
                        "jdbc:gbasedbt-sqli://127.0.0.1:9088/:GBASEDBTSERVER=gbase01",
                        null,
                        null,
                        null)));

        assertThrows(
                IllegalArgumentException.class,
                () -> new GBase8sDialect(config(
                        "jdbc:gbasedbt-sqli://127.0.0.1:9088/testdb",
                        null,
                        null,
                        null)));

        Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("GBASEDBTSERVER", "gbase01");
        assertEquals(
                DatabaseIdentifier.GBASE8S,
                new GBase8sDialect(config(
                        "jdbc:gbasedbt-sqli://127.0.0.1:9088/testdb",
                        null,
                        properties,
                        null)).name());
    }

    private static GBase8sDialect dialect(
            String schema,
            Map<String, String> properties) {
        return new GBase8sDialect(config(baseUrl(), schema, properties, null));
    }

    private static JdbcConnectionConfig config(
            String url,
            String schema,
            Map<String, String> properties,
            String dialect) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", "com.gbasedbt.jdbc.Driver");
        values.put("username", "gbasedbt");
        if (schema != null) {
            values.put("schema", schema);
        }
        if (properties != null) {
            values.put("properties", properties);
        }
        if (dialect != null) {
            values.put("dialect", dialect);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String baseUrl() {
        return "jdbc:gbasedbt-sqli://127.0.0.1:9088/testdb:GBASEDBTSERVER=gbase01;IFX_LOCK_MODE_WAIT=10";
    }
}
