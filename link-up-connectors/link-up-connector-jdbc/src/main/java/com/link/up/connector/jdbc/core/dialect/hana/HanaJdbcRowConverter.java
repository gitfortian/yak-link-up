package com.link.up.connector.jdbc.core.dialect.hana;

import com.link.up.connector.jdbc.core.converter.AbstractJdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

/** SAP HANA JDBC row converter for bounded/offline reads. */
public final class HanaJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String name() {
        return DatabaseIdentifier.HANA;
    }
}
