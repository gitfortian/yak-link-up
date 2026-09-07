package com.link.up.connector.jdbc.core.dialect.gbase.gbase8s;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** GBase 8s bounded/offline JDBC dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class GBase8sDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.GBASE8S;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return GBase8sJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new GBase8sDialect(connectionConfig);
    }
}
