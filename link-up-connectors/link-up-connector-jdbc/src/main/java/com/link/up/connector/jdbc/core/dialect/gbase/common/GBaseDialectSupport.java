package com.link.up.connector.jdbc.core.dialect.gbase.common;

import java.util.Optional;

/**
 * Family-level helpers shared by future GBase JDBC dialects.
 *
 * <p>This class must stay product-neutral. Driver names, JDBC URL parsing, SQL generation, type
 * mapping, catalog behavior and row conversion belong to the concrete 8c/8a/8s implementations
 * unless multiple completed adapters prove that a behavior is truly shared.</p>
 */
public final class GBaseDialectSupport {

    public static final String FAMILY_NAME = "gbase";

    private GBaseDialectSupport() {
    }

    public static boolean isGBase(
            String identifier) {

        return productOf(identifier).isPresent();
    }

    public static Optional<GBaseProduct> productOf(
            String identifier) {

        return GBaseProduct.fromIdentifier(identifier);
    }
}
