package com.link.up.connector.jdbc.core.dialect.tidb;

import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * TiDB row converter for the MySQL-compatible JDBC protocol.
 */
public final class TiDbJdbcRowConverter
        extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.TIDB;
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
