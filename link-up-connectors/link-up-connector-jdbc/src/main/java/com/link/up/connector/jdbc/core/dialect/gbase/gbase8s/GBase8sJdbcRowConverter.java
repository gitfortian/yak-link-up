package com.link.up.connector.jdbc.core.dialect.gbase.gbase8s;

import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

/** GBase 8s bounded Source row converter. */
public final class GBase8sJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.GBASE8S;
    }
}
