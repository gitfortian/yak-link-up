package com.link.up.connector.jdbc.core.dialect.hana;

import com.google.auto.service.AutoService;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcDialectFactory;

/** SAP HANA dialect factory. */
@AutoService(JdbcDialectFactory.class)
public final class HanaDialectFactory implements JdbcDialectFactory {

    @Override
    public String identifier() {
        return DatabaseIdentifier.HANA;
    }

    @Override
    public boolean acceptsUrl(String url) {
        return HanaJdbcUrl.accepts(url);
    }

    @Override
    public JdbcDialect create(JdbcConnectionConfig connectionConfig) {
        return new HanaDialect(connectionConfig);
    }
}
