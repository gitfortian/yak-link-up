package com.link.up.connector.jdbc.core.dialect.gbase.gbase8a;

import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

/** GBase 8a bounded Source row converter. */
public final class GBase8aJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.GBASE8A;
    }
}
