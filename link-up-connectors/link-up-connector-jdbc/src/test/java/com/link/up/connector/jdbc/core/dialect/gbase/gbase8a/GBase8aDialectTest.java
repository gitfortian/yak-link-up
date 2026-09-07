package com.link.up.connector.jdbc.core.dialect.gbase.gbase8a;

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

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GBase8aDialectTest {

    @Test
    public void loadsDedicatedGBase8aDialectFromJdbcUrlThroughSpi() {
        JdbcDialect dialect = JdbcDialectLoader.load(config(baseUrl(), null, null));
        assertEquals(DatabaseIdentifier.GBASE8A, dialect.name());
    }

    @Test
    public void factoryOnlyAcceptsDedicatedGBase8aProtocol() {
        GBase8aDialectFactory factory = new GBase8aDialectFactory();
        assertTrue(factory.acceptsUrl(baseUrl()));
        assertFalse(factory.acceptsUrl("jdbc:gbase8c://127.0.0.1:5432/app"));
        assertFalse(factory.acceptsUrl("jdbc:gbasedbt-sqli://127.0.0.1:9088/app"));
        assertFalse(factory.acceptsUrl("jdbc:mysql://127.0.0.1:3306/app"));
    }

    @Test
    public void explicitDialectKeepsFirstClassIdentity() {
        JdbcDialect dialect = JdbcDialectLoader.load(
                config(baseUrl(), null, DatabaseIdentifier.GBASE8A));
        assertEquals(DatabaseIdentifier.GBASE8A, dialect.name());
    }

    @Test
    public void extractsDatabaseWithoutConnectionProperties() {
        assertEquals("app", GBase8aJdbcUrl.databaseName(baseUrl()));
        assertEquals(
                "sales",
                GBase8aJdbcUrl.databaseName(
                        "jdbc:gbase://10.0.0.1:5258/sales?failoverEnable=true&hostList=10.0.0.2"));
    }

    @Test
    public void usesDatabaseTablePathSemanticsAndBacktickQuoting() {
        GBase8aDialect dialect = dialect();
        assertEquals(TablePath.of("orders"), dialect.parseTablePath("orders"));
        assertEquals(
                TablePath.of("analytics", "orders"),
                dialect.parseTablePath("analytics.orders"));
        assertEquals(
                TablePath.of("Sales-DB", "Order-Items"),
                dialect.parseTablePath("`Sales-DB`.`Order-Items`"));
        assertThrows(
                IllegalArgumentException.class,
                () -> dialect.parseTablePath("vc.analytics.orders"));

        assertEquals(
                "`app`.`orders`",
                dialect.tableIdentifier(TablePath.of("orders")));
        assertEquals(
                "`archive`.`orders`",
                dialect.tableIdentifier(TablePath.of("archive", "orders")));
    }

    @Test
    public void existingTableSinkUsesPortableInsertAndStillRejectsUpsert() {
        GBase8aDialect dialect = dialect();
        Catalog catalog = dialect.createCatalog(config(baseUrl(), null, null));
        assertTrue(catalog instanceof WritableCatalog);
        assertEquals(
                "INSERT INTO `app`.`orders` (`id`, `name`) VALUES (?, ?)",
                dialect.buildInsertSql(
                        TablePath.of("app", "orders"),
                        Arrays.asList("id", "name")));
        assertFalse(dialect.buildUpsertSql(
                TablePath.of("app", "orders"),
                Arrays.asList("id", "name"),
                Collections.singletonList("id")).isPresent());

        Column column = Column.builder("name", BasicType.STRING_TYPE).build();
        assertThrows(
                UnsupportedOperationException.class,
                () -> dialect.typeMapper().toDatabaseType(column));
    }

    @Test
    public void dialectDefaultsRemoveTypeAmbiguityAndEnableBatchRewrite() {
        Map<String, String> properties = dialect().defaultConnectionProperties();
        assertEquals("false", properties.get("tinyInt1isBit"));
        assertEquals("false", properties.get("yearIsDateType"));
        assertEquals("true", properties.get("rewriteBatchedStatements"));
    }

    @Test
    public void positiveFetchSizeUsesGBaseStreamingSentinel() throws Exception {
        AtomicInteger configuredFetchSize = new AtomicInteger(0);
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    if ("setFetchSize".equals(method.getName())) {
                        configuredFetchSize.set((Integer) args[0]);
                    }
                    return defaultValue(method.getReturnType());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if ("prepareStatement".equals(method.getName())) {
                        return statement;
                    }
                    return defaultValue(method.getReturnType());
                });

        dialect().prepareReadStatement(connection, "SELECT 1", 1000);
        assertEquals(Integer.MIN_VALUE, configuredFetchSize.get());
    }

    @Test
    public void boundedSourceAdvertisesBestEffortOnly() {
        assertEquals(
                Collections.singleton(ReadConsistency.BEST_EFFORT),
                dialect().supportedReadConsistencies());
        assertEquals(
                DatabaseIdentifier.GBASE8A,
                dialect().rowConverter().name());
    }

    @Test
    public void requiresDatabaseInJdbcUrlEvenWhenGenericSchemaIsPresent() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new GBase8aDialect(
                        config("jdbc:gbase://127.0.0.1:5258", "app", null)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new GBase8aDialect(
                        config("jdbc:gbase://127.0.0.1:5258", null, null)));
    }

    private static GBase8aDialect dialect() {
        return new GBase8aDialect(config(baseUrl(), null, null));
    }

    private static JdbcConnectionConfig config(
            String url,
            String schema,
            String dialect) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("url", url);
        values.put("driver", "com.gbase.jdbc.Driver");
        values.put("username", "gbase");
        if (schema != null) {
            values.put("schema", schema);
        }
        if (dialect != null) {
            values.put("dialect", dialect);
        }
        return JdbcConnectionConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static String baseUrl() {
        return "jdbc:gbase://127.0.0.1:5258/app?characterEncoding=utf8";
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
