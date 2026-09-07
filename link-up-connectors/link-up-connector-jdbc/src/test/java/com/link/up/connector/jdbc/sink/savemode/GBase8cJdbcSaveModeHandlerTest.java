package com.link.up.connector.jdbc.sink.savemode;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.WritableCatalog;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.catalog.exception.DatabaseAlreadyExistsException;
import com.link.up.api.table.catalog.exception.DatabaseNotFoundException;
import com.link.up.api.table.catalog.exception.TableAlreadyExistsException;
import com.link.up.api.table.catalog.exception.TableNotFoundException;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cCompatibilityMode;
import com.link.up.connector.jdbc.sink.DataSaveMode;
import com.link.up.connector.jdbc.sink.SchemaSaveMode;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GBase8cJdbcSaveModeHandlerTest {

    @Test
    public void ordinaryExistingTargetDoesNotResolveCompatibilityMode() {
        CatalogTable source = table(BasicType.LONG_TYPE);
        FakeCatalog catalog = new FakeCatalog(source, true);
        AtomicInteger resolutions = new AtomicInteger();
        GBase8cJdbcSaveModeHandler handler = handler(
                catalog,
                source,
                resolutions,
                GBase8cCompatibilityMode.A);

        handler.open();
        try {
            handler.handleSaveMode();
        } finally {
            handler.close();
        }

        assertEquals(0, resolutions.get());
        assertFalse(handler.isTableCreated());
    }

    @Test
    public void missingTargetResolvesModeBeforeCreatingTable() {
        CatalogTable source = table(BasicType.LONG_TYPE);
        FakeCatalog catalog = new FakeCatalog(source, false);
        AtomicInteger resolutions = new AtomicInteger();
        GBase8cJdbcSaveModeHandler handler = handler(
                catalog,
                source,
                resolutions,
                GBase8cCompatibilityMode.PG);

        handler.open();
        try {
            handler.handleSaveMode();
        } finally {
            handler.close();
        }

        assertEquals(1, resolutions.get());
        assertTrue(handler.isTableCreated());
        assertTrue(catalog.createTableCalled);
        assertEquals(GBase8cCompatibilityMode.PG, handler.getResolvedCompatibilityMode());
    }

    @Test
    public void aModeExistingPhysicalTimestampValidatesLogicalDate() {
        CatalogTable source = table(BasicType.DATE_TYPE);
        CatalogTable physicalTarget = table(BasicType.TIMESTAMP_TYPE);
        FakeCatalog catalog = new FakeCatalog(physicalTarget, true);
        AtomicInteger resolutions = new AtomicInteger();
        GBase8cJdbcSaveModeHandler handler = handler(
                catalog,
                source,
                resolutions,
                GBase8cCompatibilityMode.A);

        handler.open();
        try {
            handler.handleSaveMode();
        } finally {
            handler.close();
        }

        assertEquals(1, resolutions.get());
        assertFalse(handler.isTableCreated());
        assertEquals(GBase8cCompatibilityMode.A, handler.getResolvedCompatibilityMode());
    }

    private static GBase8cJdbcSaveModeHandler handler(
            FakeCatalog catalog,
            CatalogTable source,
            AtomicInteger resolutions,
            GBase8cCompatibilityMode mode) {
        return new GBase8cJdbcSaveModeHandler(
                SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                DataSaveMode.APPEND_DATA,
                catalog,
                source,
                true,
                () -> {
                    resolutions.incrementAndGet();
                    return mode;
                });
    }

    private static CatalogTable table(com.link.up.api.table.type.FluxDataType<?> type) {
        TableSchema schema = TableSchema.builder()
                .columns(Collections.singletonList(
                        Column.builder("value", type)
                                .nullable(true)
                                .build()))
                .build();
        return CatalogTable.builder(
                        TablePath.of("app", "public", "orders"),
                        schema)
                .build();
    }

    private static final class FakeCatalog implements WritableCatalog {
        private final CatalogTable table;
        private final boolean exists;
        private boolean createTableCalled;

        private FakeCatalog(CatalogTable table, boolean exists) {
            this.table = table;
            this.exists = exists;
        }

        @Override
        public String name() {
            return "gbase8c-test";
        }

        @Override
        public void open() {
        }

        @Override
        public void close() {
        }

        @Override
        public List<String> listDatabases() {
            return Collections.singletonList("app");
        }

        @Override
        public List<TablePath> listTables(String databaseName, String schemaName) {
            return Collections.singletonList(table.getTablePath());
        }

        @Override
        public boolean tableExists(TablePath tablePath) {
            return exists;
        }

        @Override
        public CatalogTable getTable(TablePath tablePath)
                throws CatalogException, TableNotFoundException {
            if (!exists) {
                throw new TableNotFoundException(name(), tablePath);
            }
            return table;
        }

        @Override
        public void createDatabase(String databaseName, boolean ignoreIfExists)
                throws CatalogException, DatabaseAlreadyExistsException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropDatabase(String databaseName, boolean ignoreIfNotExists)
                throws CatalogException, DatabaseNotFoundException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createTable(CatalogTable table, boolean ignoreIfExists)
                throws CatalogException, DatabaseNotFoundException, TableAlreadyExistsException {
            createTableCalled = true;
        }

        @Override
        public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
                throws CatalogException, TableNotFoundException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
                throws CatalogException, TableNotFoundException {
        }
    }
}
