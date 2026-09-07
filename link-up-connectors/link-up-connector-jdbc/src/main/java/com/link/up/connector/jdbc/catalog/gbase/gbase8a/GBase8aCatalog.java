package com.link.up.connector.jdbc.catalog.gbase.gbase8a;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.WritableCatalog;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.catalog.exception.DatabaseAlreadyExistsException;
import com.link.up.api.table.catalog.exception.DatabaseNotFoundException;
import com.link.up.api.table.catalog.exception.TableAlreadyExistsException;
import com.link.up.api.table.catalog.exception.TableNotFoundException;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8a.GBase8aJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8a.GBase8aTypeMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * GBase 8a Catalog for bounded/offline Source and JDBC Sink jobs.
 *
 * <p>Metadata uses the standard JDBC {@link DatabaseMetaData} contract. The Sink can safely create
 * a missing target table from Link-Up schema metadata and truncate existing data. Database DDL,
 * destructive table recreation and runtime schema evolution stay disabled; the automatic-target
 * path never guesses MPP distribution, partition or replication design.</p>
 */
public final class GBase8aCatalog implements WritableCatalog {

    public static final String TABLE_OPTION_DIALECT = "dialect";

    private final String catalogName;
    private final JdbcCatalogConfig config;
    private final String defaultDatabase;
    private final GBase8aTypeMapper typeMapper = new GBase8aTypeMapper();
    private volatile boolean opened;

    public GBase8aCatalog(
            String catalogName,
            JdbcCatalogConfig config,
            String defaultDatabase) {
        if (!hasText(catalogName)) {
            throw new IllegalArgumentException("catalogName must not be empty");
        }
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (!GBase8aJdbcUrl.accepts(config.getUrl())) {
            throw new IllegalArgumentException("非法 GBase 8a JDBC URL：" + config.getUrl());
        }
        if (!hasText(defaultDatabase)) {
            throw new IllegalArgumentException("GBase 8a JDBC URL 必须指定默认 database");
        }
        this.catalogName = catalogName.trim();
        this.config = config;
        this.defaultDatabase = defaultDatabase.trim();
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public synchronized void open() throws CatalogException {
        if (opened) {
            return;
        }
        loadDriver();
        try (Connection connection = newConnection()) {
            if (!connection.isValid(5)) {
                throw new CatalogException("GBase 8a Catalog 连接校验失败：" + config.getUrl());
            }
            opened = true;
        } catch (SQLException e) {
            throw new CatalogException("GBase 8a Catalog 连接失败：" + config.getUrl(), e);
        }
    }

    @Override
    public synchronized void close() {
        opened = false;
    }

    @Override
    public Optional<String> getDefaultDatabase() {
        return Optional.of(defaultDatabase);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        checkOpened();
        try (Connection connection = newConnection();
             ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            Set<String> databases = new LinkedHashSet<String>();
            while (resultSet.next()) {
                String database = resultSet.getString(1);
                if (hasText(database)) {
                    databases.add(database.trim());
                }
            }
            databases.add(defaultDatabase);
            return new ArrayList<String>(databases);
        } catch (SQLException e) {
            throw new CatalogException("获取 GBase 8a 数据库列表失败", e);
        }
    }

    /** GBase 8a models database as JDBC catalog and has no separate schema layer. */
    @Override
    public List<String> listSchemas(String databaseName) {
        return Collections.emptyList();
    }

    @Override
    public List<TablePath> listTables(
            String databaseName,
            String schemaName) throws CatalogException {
        checkOpened();
        String database = resolveDatabase(databaseName, schemaName);
        try (Connection connection = newConnection();
             ResultSet resultSet = connection.getMetaData()
                     .getTables(database, null, "%", null)) {
            List<TablePath> tables = new ArrayList<TablePath>();
            while (resultSet.next()) {
                String tableType = resultSet.getString("TABLE_TYPE");
                if (!isBaseTable(tableType)) {
                    continue;
                }
                String tableName = resultSet.getString("TABLE_NAME");
                if (hasText(tableName)) {
                    tables.add(TablePath.of(database, tableName.trim()));
                }
            }
            return tables;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 GBase 8a 表列表失败，database=" + database,
                    e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkOpened();
        TablePath normalized = normalizeTablePath(tablePath);
        try (Connection connection = newConnection();
             ResultSet resultSet = connection.getMetaData().getTables(
                     normalized.getDatabaseName(),
                     null,
                     normalized.getTableName(),
                     null)) {
            while (resultSet.next()) {
                if (isBaseTable(resultSet.getString("TABLE_TYPE"))
                        && normalized.getTableName().equals(resultSet.getString("TABLE_NAME"))) {
                    return true;
                }
            }
            return false;
        } catch (SQLException e) {
            throw new CatalogException("检查 GBase 8a 表是否存在失败，table=" + normalized, e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        TablePath normalized = normalizeTablePath(tablePath);
        try (Connection connection = newConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            List<Column> columns = readColumns(metadata, normalized);
            if (columns.isEmpty()) {
                throw new TableNotFoundException(catalogName, normalized);
            }
            PrimaryKey primaryKey = readPrimaryKey(metadata, normalized);
            TableSchema schema = TableSchema.builder()
                    .columns(columns)
                    .primaryKey(primaryKey)
                    .build();
            return CatalogTable.builder(normalized, schema)
                    .option(TABLE_OPTION_DIALECT, DatabaseIdentifier.GBASE8A)
                    .build();
        } catch (TableNotFoundException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException("获取 GBase 8a 表结构失败，table=" + normalized, e);
        }
    }

    @Override
    public void createDatabase(
            String databaseName,
            boolean ignoreIfExists)
            throws CatalogException, DatabaseAlreadyExistsException {
        throw unsupportedSchemaDdl("create database");
    }

    @Override
    public void dropDatabase(
            String databaseName,
            boolean ignoreIfNotExists)
            throws CatalogException, DatabaseNotFoundException {
        throw unsupportedSchemaDdl("drop database");
    }

    /**
     * Creates a safe baseline target table. Distribution, partitioning, defaults, identity and
     * other source physical-design semantics are intentionally not inferred here.
     */
    @Override
    public void createTable(
            CatalogTable table,
            boolean ignoreIfExists)
            throws CatalogException, DatabaseNotFoundException, TableAlreadyExistsException {
        checkOpened();
        if (table == null) {
            throw new IllegalArgumentException("table must not be null");
        }

        TablePath normalized = normalizeTablePath(table.getTablePath());
        requireCurrentDatabase(normalized.getDatabaseName());
        if (tableExists(normalized)) {
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistsException(catalogName, normalized);
        }

        CatalogTable ddlTable = table.getTablePath().equals(normalized)
                ? table
                : table.withPath(normalized);
        String sql = new GBase8aCreateTableSqlBuilder(
                normalized,
                ddlTable,
                typeMapper)
                .build();

        try (Connection connection = newConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new CatalogException("创建 GBase 8a 目标表失败，table=" + normalized, e);
        }
    }

    @Override
    public void addColumn(
            TablePath tablePath,
            Column column)
            throws CatalogException, TableNotFoundException {
        throw unsupportedSchemaDdl("add column");
    }

    @Override
    public void dropTable(
            TablePath tablePath,
            boolean ignoreIfNotExists)
            throws CatalogException, TableNotFoundException {
        throw unsupportedSchemaDdl("drop table");
    }

    @Override
    public void truncateTable(
            TablePath tablePath,
            boolean ignoreIfNotExists)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        TablePath normalized = normalizeTablePath(tablePath);
        requireCurrentDatabase(normalized.getDatabaseName());
        if (!tableExists(normalized)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotFoundException(catalogName, normalized);
        }

        String sql = "TRUNCATE TABLE " + quoteTable(normalized);
        try (Connection connection = newConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new CatalogException("清空 GBase 8a 表失败，table=" + normalized, e);
        }
    }

    private List<Column> readColumns(
            DatabaseMetaData metadata,
            TablePath tablePath) throws SQLException {
        try (ResultSet resultSet = metadata.getColumns(
                tablePath.getDatabaseName(),
                null,
                tablePath.getTableName(),
                "%")) {
            List<Column> columns = new ArrayList<Column>();
            while (resultSet.next()) {
                columns.add(typeMapper.toColumn(resultSet));
            }
            return columns;
        }
    }

    private PrimaryKey readPrimaryKey(
            DatabaseMetaData metadata,
            TablePath tablePath) throws SQLException {
        try (ResultSet resultSet = metadata.getPrimaryKeys(
                tablePath.getDatabaseName(),
                null,
                tablePath.getTableName())) {
            String primaryKeyName = null;
            Map<Integer, String> columnsBySequence = new TreeMap<Integer, String>();
            while (resultSet.next()) {
                if (primaryKeyName == null) {
                    primaryKeyName = resultSet.getString("PK_NAME");
                }
                String column = resultSet.getString("COLUMN_NAME");
                int sequence = resultSet.getInt("KEY_SEQ");
                if (hasText(column)) {
                    columnsBySequence.put(sequence, column.trim());
                }
            }
            return columnsBySequence.isEmpty()
                    ? null
                    : PrimaryKey.of(
                            primaryKeyName,
                            new ArrayList<String>(columnsBySequence.values()));
        }
    }

    private TablePath normalizeTablePath(TablePath tablePath) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        if (!hasText(tablePath.getTableName())) {
            throw new IllegalArgumentException("table name must not be empty");
        }
        String database = resolveDatabase(
                tablePath.getDatabaseName(),
                tablePath.getSchemaName());
        return TablePath.of(database, tablePath.getTableName().trim());
    }

    private String resolveDatabase(String databaseName, String schemaName) {
        if (hasText(databaseName)) {
            return databaseName.trim();
        }
        if (hasText(schemaName)) {
            return schemaName.trim();
        }
        return defaultDatabase;
    }

    private void requireCurrentDatabase(String databaseName) {
        if (!hasText(databaseName)
                || !defaultDatabase.equalsIgnoreCase(databaseName.trim())) {
            throw new IllegalArgumentException(
                    "GBase 8a automatic-target JDBC stage does not support cross-database DDL; "
                            + "urlDatabase="
                            + defaultDatabase
                            + ", targetDatabase="
                            + databaseName);
        }
    }

    private String quoteTable(TablePath tablePath) {
        return quoteIdentifier(tablePath.getDatabaseName())
                + "."
                + quoteIdentifier(tablePath.getTableName());
    }

    private static String quoteIdentifier(String identifier) {
        if (!hasText(identifier)) {
            throw new IllegalArgumentException("identifier must not be empty");
        }
        return "`" + identifier.trim().replace("`", "``") + "`";
    }

    private Connection newConnection() throws SQLException {
        return DriverManager.getConnection(config.getUrl(), config.toConnectionProperties());
    }

    private void loadDriver() {
        if (!hasText(config.getDriverClass())) {
            return;
        }
        try {
            Class.forName(config.getDriverClass());
        } catch (ClassNotFoundException e) {
            throw new CatalogException("找不到 GBase 8a JDBC Driver：" + config.getDriverClass(), e);
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("Catalog 尚未打开，请先调用 open()");
        }
    }

    private static CatalogException unsupportedSchemaDdl(String operation) {
        return new CatalogException(
                "GBase 8a automatic-target JDBC stage does not support "
                        + operation
                        + "; only safe CREATE TABLE for a missing target and TRUNCATE are enabled");
    }

    private static boolean isBaseTable(String tableType) {
        return tableType != null
                && ("TABLE".equalsIgnoreCase(tableType)
                || "BASE TABLE".equalsIgnoreCase(tableType));
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
