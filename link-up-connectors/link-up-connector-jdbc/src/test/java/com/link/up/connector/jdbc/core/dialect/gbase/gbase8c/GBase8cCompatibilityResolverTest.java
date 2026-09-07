package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GBase8cCompatibilityResolverTest {

    @Test
    public void resolvesModeFromCurrentDatabaseCatalogRow() throws Exception {
        AtomicReference<String> preparedSql = new AtomicReference<String>();
        Connection connection = connection("B", true, preparedSql);

        assertEquals(
                GBase8cCompatibilityMode.B,
                GBase8cCompatibilityResolver.resolve(connection));
        assertEquals(
                "SELECT datcompatibility FROM pg_database WHERE datname = current_database()",
                preparedSql.get());
    }

    @Test
    public void rejectsUnknownModeWithoutPostgresqlFallback() {
        SQLException error = assertThrows(
                SQLException.class,
                () -> GBase8cCompatibilityResolver.resolve(
                        connection("MSSQL", true, new AtomicReference<String>())));

        assertTrue(error.getMessage().contains("Unsupported GBase 8c compatibility mode"));
        assertTrue(error.getCause() instanceof IllegalArgumentException);
    }

    @Test
    public void failsWhenCurrentDatabaseRowIsMissing() {
        SQLException error = assertThrows(
                SQLException.class,
                () -> GBase8cCompatibilityResolver.resolve(
                        connection(null, false, new AtomicReference<String>())));

        assertTrue(error.getMessage().contains("Unable to resolve"));
    }

    private static Connection connection(
            String value,
            boolean hasRow,
            AtomicReference<String> preparedSql) {
        ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(
                GBase8cCompatibilityResolverTest.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                new java.lang.reflect.InvocationHandler() {
                    private boolean advanced;

                    @Override
                    public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
                        if ("next".equals(method.getName())) {
                            if (advanced || !hasRow) {
                                return false;
                            }
                            advanced = true;
                            return true;
                        }
                        if ("getString".equals(method.getName())) {
                            return value;
                        }
                        return defaultValue(method.getReturnType());
                    }
                });

        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                GBase8cCompatibilityResolverTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    if ("executeQuery".equals(method.getName())) {
                        return resultSet;
                    }
                    return defaultValue(method.getReturnType());
                });

        return (Connection) Proxy.newProxyInstance(
                GBase8cCompatibilityResolverTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if ("prepareStatement".equals(method.getName())) {
                        preparedSql.set((String) args[0]);
                        return statement;
                    }
                    return defaultValue(method.getReturnType());
                });
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
