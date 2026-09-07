package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import com.link.up.api.table.catalog.Column;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;
import com.link.up.connector.jdbc.core.dialect.postgres.PostgresTypeMapper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * GBase 8c type mapper for bounded Source/Sink jobs.
 *
 * <p>The adapter keeps PostgreSQL-compatible metadata mapping for reads. Target DDL mapping requires
 * an explicitly resolved {@link GBase8cCompatibilityMode}; the mode-less JdbcTypeMapper method
 * remains blocked so callers cannot accidentally assume PG semantics.</p>
 */
public final class GBase8cTypeMapper implements JdbcTypeMapper {

    private final PostgresTypeMapper delegate = new PostgresTypeMapper();

    @Override
    public Column map(ResultSetMetaData metadata, int columnIndex) throws SQLException {
        return delegate.map(metadata, columnIndex);
    }

    /** Maps one information_schema.columns row using the PG-compatible read contract. */
    public Column toColumn(ResultSet row) throws SQLException {
        return delegate.toColumn(row);
    }

    @Override
    public String toDatabaseType(Column column) {
        throw new UnsupportedOperationException(
                "GBase 8c target DDL requires a resolved compatibility mode; "
                        + "use toDatabaseType(column, compatibilityMode)");
    }

    public String toDatabaseType(
            Column column,
            GBase8cCompatibilityMode compatibilityMode) {
        return new GBase8cTargetTypeMapper(compatibilityMode)
                .toDatabaseType(column);
    }
}
