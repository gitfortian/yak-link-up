package com.link.up.connector.jdbc.core.dialect.goldendb;

import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * GoldenDB row converter for the MySQL-compatible JDBC protocol.
 */
public final class GoldenDbJdbcRowConverter
        extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.GOLDENDB;
    }

    @Override
    protected void writeTime(
            PreparedStatement statement,
            int index,
            LocalTime value)
            throws SQLException {

        statement.setTimestamp(
                index,
                java.sql.Timestamp.valueOf(
                        LocalDateTime.of(
                                LocalDate.now(),
                                value)));
    }
}
