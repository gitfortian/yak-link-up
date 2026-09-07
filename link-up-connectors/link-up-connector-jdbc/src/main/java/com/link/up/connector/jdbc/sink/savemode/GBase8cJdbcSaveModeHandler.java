package com.link.up.connector.jdbc.sink.savemode;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.WritableCatalog;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cCompatibilityMode;
import com.link.up.connector.jdbc.sink.DataSaveMode;
import com.link.up.connector.jdbc.sink.SchemaSaveMode;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * GBase 8c save-mode handler with lazy compatibility-mode resolution.
 *
 * <p>Existing-table jobs stay on the generic validation path unless a documented A-mode
 * DATE/TIMESTAMP physical mismatch is present. Missing-table creation resolves compatibility mode
 * before DDL, so automatic CREATE TABLE can never run under an implicit PostgreSQL assumption.</p>
 */
public final class GBase8cJdbcSaveModeHandler extends JdbcSaveModeHandler {

    private final Supplier<GBase8cCompatibilityMode> compatibilityModeResolver;
    private GBase8cCompatibilityMode resolvedCompatibilityMode;

    public GBase8cJdbcSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            WritableCatalog catalog,
            CatalogTable table,
            boolean createPrimaryKey,
            Supplier<GBase8cCompatibilityMode> compatibilityModeResolver) {
        super(schemaSaveMode, dataSaveMode, catalog, table, createPrimaryKey);
        this.compatibilityModeResolver = Objects.requireNonNull(
                compatibilityModeResolver,
                "compatibilityModeResolver must not be null");
    }

    @Override
    protected void createTable() {
        // Resolve first so automatic DDL always has an explicit, verified database mode.
        compatibilityMode();
        super.createTable();
    }

    @Override
    protected TableSchema normalizeTargetSchemaForValidation(
            TableSchema sourceSchema,
            TableSchema targetSchema) {
        if (!GBase8cSchemaCompatibility.hasDateTimestampCandidate(sourceSchema, targetSchema)) {
            return targetSchema;
        }
        return GBase8cSchemaCompatibility.normalizeTargetForValidation(
                sourceSchema,
                targetSchema,
                compatibilityMode());
    }

    /** Returns the mode only when this handler already had a reason to resolve it. */
    public GBase8cCompatibilityMode getResolvedCompatibilityMode() {
        return resolvedCompatibilityMode;
    }

    private GBase8cCompatibilityMode compatibilityMode() {
        if (resolvedCompatibilityMode == null) {
            resolvedCompatibilityMode = Objects.requireNonNull(
                    compatibilityModeResolver.get(),
                    "GBase 8c compatibility resolver returned null");
        }
        return resolvedCompatibilityMode;
    }
}
