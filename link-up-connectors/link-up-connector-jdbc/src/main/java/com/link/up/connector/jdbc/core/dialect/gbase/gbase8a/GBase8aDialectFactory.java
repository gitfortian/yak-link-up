package com.link.up.connector.jdbc.core.dialect.gbase.gbase8a;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** GBase 8a bounded JDBC Source dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class GBase8aDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.GBASE8A;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return GBase8aJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new GBase8aDialect(connectionConfig);
    }
}
