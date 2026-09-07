package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Resolves the current GBase 8c database compatibility mode from pg_database. */
public final class GBase8cCompatibilityResolver {

    static final String RESOLVE_SQL =
            "SELECT datcompatibility FROM pg_database WHERE datname = current_database()";

    private GBase8cCompatibilityResolver() {
    }

    public static GBase8cCompatibilityMode resolve(Connection connection) throws SQLException {
        if (connection == null) {
            throw new IllegalArgumentException("connection must not be null");
        }
        try (PreparedStatement statement = connection.prepareStatement(RESOLVE_SQL);
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                throw new SQLException(
                        "Unable to resolve GBase 8c datcompatibility for current database");
            }
            String value = resultSet.getString(1);
            try {
                return GBase8cCompatibilityMode.fromDatabaseValue(value);
            } catch (IllegalArgumentException e) {
                throw new SQLException(
                        "Unsupported GBase 8c compatibility mode for automatic target DDL: "
                                + value,
                        e);
            }
        }
    }
}
