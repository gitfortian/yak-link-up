package com.link.up.connector.jdbc.core.dialect.goldendb;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/**
 * GoldenDB JDBC dialect SPI.
 *
 * <p>GoldenDB can use the MySQL-compatible JDBC protocol and therefore shares
 * the {@code jdbc:mysql://} URL scheme with MySQL and TiDB. URL-only detection
 * is intentionally disabled; select GoldenDB explicitly with
 * {@code dialect=goldendb}.</p>
 */
@AutoService(JdbcDialectFactory.class)
public final class GoldenDbDialectFactory
        implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.GOLDENDB;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return false;
    }

    @Override
    public JdbcDialect create(
            JdbcConnectionConfig connectionConfig) {

        return new GoldenDbDialect(connectionConfig);
    }
}
