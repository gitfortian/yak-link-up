package com.link.up.connector.jdbc.core.dialect.gbase.common;

import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.util.Locale;
import java.util.Optional;

/**
 * Stable product identities for the GBase database family.
 *
 * <p>The family intentionally does not expose a generic {@code gbase} dialect. GBase 8c, 8a and
 * 8s have different JDBC protocols and database semantics, so runtime support must be implemented
 * per product while sharing only proven family-level metadata.</p>
 */
public enum GBaseProduct {

    GBASE8C(
            DatabaseIdentifier.GBASE8C,
            "GBase 8c"),

    GBASE8A(
            DatabaseIdentifier.GBASE8A,
            "GBase 8a"),

    GBASE8S(
            DatabaseIdentifier.GBASE8S,
            "GBase 8s");

    private final String identifier;
    private final String displayName;

    GBaseProduct(
            String identifier,
            String displayName) {

        this.identifier = identifier;
        this.displayName = displayName;
    }

    public String identifier() {
        return identifier;
    }

    public String displayName() {
        return displayName;
    }

    public static Optional<GBaseProduct> fromIdentifier(
            String identifier) {

        if (identifier == null) {
            return Optional.empty();
        }

        String normalized =
                identifier.trim()
                        .toLowerCase(Locale.ROOT);

        if (normalized.isEmpty()) {
            return Optional.empty();
        }

        for (GBaseProduct product : values()) {
            if (product.identifier.equals(normalized)) {
                return Optional.of(product);
            }
        }

        return Optional.empty();
    }
}
