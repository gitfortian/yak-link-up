package com.link.up.connector.jdbc.core.dialect.tidb;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/**
 * TiDB JDBC dialect SPI.
 *
 * <p>TiDB speaks the MySQL protocol and therefore uses the same jdbc:mysql URL
 * scheme as MySQL. URL-only detection would make both factories match, so TiDB
 * must be selected explicitly with {@code dialect=tidb}.</p>
 */
@AutoService(JdbcDialectFactory.class)
public final class TiDbDialectFactory
        implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.TIDB;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return false;
    }

    @Override
    public JdbcDialect create(
            JdbcConnectionConfig connectionConfig) {

        return new TiDbDialect(connectionConfig);
    }
}
