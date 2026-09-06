package com.link.up.connector.jdbc.catalog.tidb;

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
 * TiDB Catalog facade over the MySQL-compatible metadata protocol.
 *
 * <p>The facade keeps TiDB as a first-class database identity while reusing
 * the mature MySQL INFORMATION_SCHEMA and DDL implementation.</p>
 */
public final class TiDbCatalog
        implements WritableCatalog {

    public static final String DIALECT =
            DatabaseIdentifier.TIDB;

    private final WritableCatalog delegate;

    public TiDbCatalog(
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

        delegate.createDatabase(
                databaseName,
                ignoreIfExists);
    }

    @Override
    public void dropDatabase(
            String databaseName,
            boolean ignoreIfNotExists)
            throws CatalogException,
            DatabaseNotFoundException {

        delegate.dropDatabase(
                databaseName,
                ignoreIfNotExists);
    }

    @Override
    public void createTable(
            CatalogTable table,
            boolean ignoreIfExists)
            throws CatalogException,
            DatabaseNotFoundException,
            TableAlreadyExistsException {

        delegate.createTable(
                asMySqlCompatibleSource(table),
                ignoreIfExists);
    }

    @Override
    public void addColumn(
            TablePath tablePath,
            Column column)
            throws CatalogException,
            TableNotFoundException {

        delegate.addColumn(
                tablePath,
                column);
    }

    @Override
    public void dropTable(
            TablePath tablePath,
            boolean ignoreIfNotExists)
            throws CatalogException,
            TableNotFoundException {

        delegate.dropTable(
                tablePath,
                ignoreIfNotExists);
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

    private static CatalogTable asMySqlCompatibleSource(
            CatalogTable table) {

        String sourceDialect =
                table.getOptions()
                        .get(MySqlCatalog.TABLE_OPTION_DIALECT);

        if (!DIALECT.equalsIgnoreCase(sourceDialect)) {
            return table;
        }

        return table.toBuilder()
                .option(
                        MySqlCatalog.TABLE_OPTION_DIALECT,
                        DatabaseIdentifier.MYSQL)
                .build();
    }
}
