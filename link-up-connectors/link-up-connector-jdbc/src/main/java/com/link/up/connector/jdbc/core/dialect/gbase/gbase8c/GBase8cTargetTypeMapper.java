package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.dialect.postgres.PostgresTypeMapper;

/**
 * Compatibility-aware target type mapping foundation for GBase 8c automatic DDL.
 *
 * <p>Where GBase 8c core scalar types round-trip cleanly through the existing PG-compatible
 * metadata contract, this mapper intentionally keeps those portable core types instead of copying
 * source-vendor aliases. Compatibility mode is used only where the target database changes the
 * actual semantics.</p>
 */
public final class GBase8cTargetTypeMapper {

    private final GBase8cCompatibilityMode compatibilityMode;
    private final PostgresTypeMapper portableMapper = new PostgresTypeMapper();

    public GBase8cTargetTypeMapper(GBase8cCompatibilityMode compatibilityMode) {
        if (compatibilityMode == null) {
            throw new IllegalArgumentException("compatibilityMode must not be null");
        }
        this.compatibilityMode = compatibilityMode;
    }

    public GBase8cCompatibilityMode compatibilityMode() {
        return compatibilityMode;
    }

    /**
     * Maps one Flux column to a safe physical GBase 8c target type for the resolved database mode.
     */
    public String toDatabaseType(Column column) {
        if (column == null || column.getDataType() == null) {
            throw new IllegalArgumentException("column/dataType must not be null");
        }

        SqlType type = column.getDataType().getSqlType();
        switch (type) {
            case STRING:
                return stringType(column);
            case DATE:
                return dateType();
            case TIMESTAMP_TZ:
                if (compatibilityMode == GBase8cCompatibilityMode.PG) {
                    return portableMapper.toDatabaseType(column);
                }
                throw unsupported(
                        column,
                        "TIMESTAMP_TZ automatic DDL is only verified for PG compatibility mode");
            case ARRAY:
            case MAP:
            case ROW:
            case NULL:
                throw unsupported(
                        column,
                        "automatic DDL foundation only supports scalar relational types");
            default:
                return portableMapper.toDatabaseType(column);
        }
    }

    private String stringType(Column column) {
        if (compatibilityMode == GBase8cCompatibilityMode.PG) {
            return portableMapper.toDatabaseType(column);
        }

        /*
         * GBase 8c documents PG-mode CHAR/VARCHAR lengths as characters while the other stable
         * compatibility modes count bytes. Source metadata lengths are not guaranteed to carry
         * byte semantics, so TEXT is the safe cross-database baseline outside PG mode.
         */
        return "TEXT";
    }

    private String dateType() {
        /*
         * In A mode GBase 8c documents DATE as TIMESTAMP(0) WITHOUT TIME ZONE internally. Emit the
         * physical contract explicitly so DDL preview and later metadata normalization can agree.
         */
        return compatibilityMode == GBase8cCompatibilityMode.A
                ? "TIMESTAMP(0) WITHOUT TIME ZONE"
                : "DATE";
    }

    private UnsupportedOperationException unsupported(Column column, String reason) {
        return new UnsupportedOperationException(
                "GBase 8c cannot map target column '"
                        + column.getName()
                        + "' from Flux type "
                        + column.getDataType().getSqlType()
                        + " in compatibility mode "
                        + compatibilityMode.databaseValue()
                        + ": "
                        + reason);
    }
}
