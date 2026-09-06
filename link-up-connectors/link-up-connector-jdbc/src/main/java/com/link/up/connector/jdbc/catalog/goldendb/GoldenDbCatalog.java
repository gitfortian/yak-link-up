package com.link.up.connector.jdbc.catalog.goldendb;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.WritableCatalog;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.catalog.exception.DatabaseAlreadyExistsException;
import com.link.up.api.table.catalog.exception.DatabaseNotFoundException;
import com.link.up.api.table.catalog.exception.TableAlreadyExistsException;
import com.link.up.api.table.catalog.exception.TableNotFoundException;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.mysql.MySqlCatalog;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;

import java.util.List;
import java.util.Optional;

/**
 * GoldenDB catalog facade over MySQL-compatible JDBC metadata.
 *
 * <p>Stage 1 deliberately supports metadata discovery and writes to existing
 * tables only. Schema-changing DDL is blocked until GoldenDB distribution and
 * sharding semantics can be modeled explicitly instead of silently creating a
 * generic MySQL-style table.</p>
 */
public final class GoldenDbCatalog
        implements WritableCatalog {

    public static final String DIALECT =
            DatabaseIdentifier.GOLDENDB;

    private final WritableCatalog delegate;

    public GoldenDbCatalog(
            String catalogName,
            JdbcCatalogConfig config) {

        this.delegate =
                new MySqlCatalog(
                        catalogName,
                        config);
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void open() throws CatalogException {
        delegate.open();
    }

    @Override
    public void close() throws CatalogException {
        delegate.close();
    }

    @Override
    public Optional<String> getDefaultDatabase()
            throws CatalogException {

        return delegate.getDefaultDatabase();
    }

    @Override
    public List<String> listDatabases()
            throws CatalogException {

        return delegate.listDatabases();
    }

    @Override
    public List<String> listSchemas(
            String databaseName)
            throws CatalogException {

        return delegate.listSchemas(databaseName);
    }

    @Override
    public List<TablePath> listTables(
            String databaseName,
            String schemaName)
            throws CatalogException {

        return delegate.listTables(
                databaseName,
                schemaName);
    }

    @Override
    public boolean tableExists(
            TablePath tablePath)
            throws CatalogException {

        return delegate.tableExists(tablePath);
    }

    @Override
    public CatalogTable getTable(
            TablePath tablePath)
            throws CatalogException,
            TableNotFoundException {

        CatalogTable table =
                delegate.getTable(tablePath);

        return table.toBuilder()
                .option(
                        MySqlCatalog.TABLE_OPTION_DIALECT,
                        DIALECT)
                .build();
    }

    @Override
    public void createDatabase(
            String databaseName,
            boolean ignoreIfExists)
            throws CatalogException,
            DatabaseAlreadyExistsException {

        throw unsupportedSchemaDdl(
                "create database");
    }

    @Override
    public void dropDatabase(
            String databaseName,
            boolean ignoreIfNotExists)
            throws CatalogException,
            DatabaseNotFoundException {

        throw unsupportedSchemaDdl(
                "drop database");
    }

    @Override
    public void createTable(
            CatalogTable table,
            boolean ignoreIfExists)
            throws CatalogException,
            DatabaseNotFoundException,
            TableAlreadyExistsException {

        throw unsupportedSchemaDdl(
                "create table");
    }

    @Override
    public void addColumn(
            TablePath tablePath,
            Column column)
            throws CatalogException,
            TableNotFoundException {

        throw unsupportedSchemaDdl(
                "add column");
    }

    @Override
    public void dropTable(
            TablePath tablePath,
            boolean ignoreIfNotExists)
            throws CatalogException,
            TableNotFoundException {

        throw unsupportedSchemaDdl(
                "drop table");
    }

    @Override
    public void truncateTable(
            TablePath tablePath,
            boolean ignoreIfNotExists)
            throws CatalogException,
            TableNotFoundException {

        delegate.truncateTable(
                tablePath,
                ignoreIfNotExists);
    }

    private static CatalogException unsupportedSchemaDdl(
            String operation) {

        return new CatalogException(
                "GoldenDB Stage 1 supports existing target tables only; "
                        + operation
                        + " is disabled until GoldenDB distribution/sharding DDL is modeled");
    }
}
