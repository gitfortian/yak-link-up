package com.link.up.connector.jdbc.catalog.gbase.gbase8s;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.catalog.exception.TableNotFoundException;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8s.GBase8sJdbcUrl;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8s.GBase8sTypeMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
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
 * Read-only GBase 8s Catalog for bounded/offline Source jobs.
 *
 * <p>GBase 8s JDBC exposes database as catalog and table owner as schema. Stage 1 binds one Catalog
 * instance to the database in the JDBC URL and uses standard {@link DatabaseMetaData} for owner,
 * table, column and primary-key discovery. Cross-database rebinding and writable DDL remain out of
 * scope.</p>
 */
public final class GBase8sCatalog implements Catalog {

    public static final String TABLE_OPTION_DIALECT = "dialect";

    private static final String[] BASE_TABLE_TYPES = new String[]{"TABLE"};

    private final String catalogName;
    private final JdbcCatalogConfig config;
    private final String defaultDatabase;
    private final String defaultOwner;
    private final GBase8sTypeMapper typeMapper = new GBase8sTypeMapper();
    private volatile boolean opened;

    public GBase8sCatalog(
            String catalogName,
            JdbcCatalogConfig config,
            String defaultDatabase,
            String defaultOwner) {
        if (!hasText(catalogName)) {
            throw new IllegalArgumentException("catalogName must not be empty");
        }
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (!GBase8sJdbcUrl.accepts(config.getUrl())) {
            throw new IllegalArgumentException("非法 GBase 8s JDBC URL：" + config.getUrl());
        }
        if (!hasText(defaultDatabase)) {
            throw new IllegalArgumentException("GBase 8s Stage 1 必须指定默认 database");
        }
        this.catalogName = catalogName.trim();
        this.config = config;
        this.defaultDatabase = defaultDatabase.trim();
        this.defaultOwner = hasText(defaultOwner) ? defaultOwner.trim() : null;
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
                throw new CatalogException("GBase 8s Catalog 连接校验失败：" + config.getUrl());
            }
            opened = true;
        } catch (SQLException e) {
            throw new CatalogException("GBase 8s Catalog 连接失败：" + config.getUrl(), e);
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

    /** Stage 1 keeps one physical JDBC connection bound to one database. */
    @Override
    public List<String> listDatabases() throws CatalogException {
        checkOpened();
        return Collections.singletonList(defaultDatabase);
    }

    /** GBase 8s JDBC reports table owner through TABLE_SCHEM. */
    @Override
    public List<String> listSchemas(String databaseName) throws CatalogException {
        checkOpened();
        requireCurrentDatabase(databaseName);
        try (Connection connection = newConnection();
             ResultSet resultSet = connection.getMetaData().getTables(
                     defaultDatabase,
                     null,
                     "%",
                     BASE_TABLE_TYPES)) {
            Set<String> owners = new LinkedHashSet<String>();
            while (resultSet.next()) {
                String owner = resultSet.getString("TABLE_SCHEM");
                if (hasText(owner)) {
                    owners.add(owner.trim());
                }
            }
            if (hasText(defaultOwner)) {
                owners.add(defaultOwner);
            }
            return new ArrayList<String>(owners);
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 GBase 8s Owner 列表失败，database=" + defaultDatabase,
                    e);
        }
    }

    @Override
    public List<TablePath> listTables(
            String databaseName,
            String schemaName) throws CatalogException {
        checkOpened();
        requireCurrentDatabase(databaseName);
        String owner = hasText(schemaName)
                ? schemaName.trim()
                : defaultOwner;

        try (Connection connection = newConnection();
             ResultSet resultSet = connection.getMetaData().getTables(
                     defaultDatabase,
                     owner,
                     "%",
                     BASE_TABLE_TYPES)) {
            List<TablePath> tables = new ArrayList<TablePath>();
            while (resultSet.next()) {
                if (!isBaseTable(resultSet.getString("TABLE_TYPE"))) {
                    continue;
                }
                String tableName = resultSet.getString("TABLE_NAME");
                if (!hasText(tableName)) {
                    continue;
                }
                String resolvedOwner = resultSet.getString("TABLE_SCHEM");
                tables.add(TablePath.of(
                        defaultDatabase,
                        hasText(resolvedOwner) ? resolvedOwner.trim() : owner,
                        tableName.trim()));
            }
            return tables;
        } catch (SQLException e) {
            throw new CatalogException(
                    "获取 GBase 8s 表列表失败，database="
                            + defaultDatabase
                            + ", owner="
                            + owner,
                    e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkOpened();
        try (Connection connection = newConnection()) {
            TablePath normalized = normalizeTablePath(connection, tablePath, false);
            return normalized != null;
        } catch (SQLException e) {
            throw new CatalogException("检查 GBase 8s 表是否存在失败，table=" + tablePath, e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotFoundException {
        checkOpened();
        try (Connection connection = newConnection()) {
            TablePath normalized = normalizeTablePath(connection, tablePath, true);
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
                    .option(TABLE_OPTION_DIALECT, DatabaseIdentifier.GBASE8S)
                    .build();
        } catch (TableNotFoundException e) {
            throw e;
        } catch (SQLException e) {
            throw new CatalogException("获取 GBase 8s 表结构失败，table=" + tablePath, e);
        }
    }

    private List<Column> readColumns(
            DatabaseMetaData metadata,
            TablePath tablePath) throws SQLException {
        try (ResultSet resultSet = metadata.getColumns(
                defaultDatabase,
                tablePath.getSchemaName(),
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
                defaultDatabase,
                tablePath.getSchemaName(),
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

    private TablePath normalizeTablePath(
            Connection connection,
            TablePath tablePath,
            boolean failWhenMissing) throws SQLException {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        requireCurrentDatabase(tablePath.getDatabaseName());
        if (!hasText(tablePath.getTableName())) {
            throw new IllegalArgumentException("table name must not be empty");
        }

        String owner = hasText(tablePath.getSchemaName())
                ? tablePath.getSchemaName().trim()
                : defaultOwner;
        String tableName = tablePath.getTableName().trim();

        try (ResultSet resultSet = connection.getMetaData().getTables(
                defaultDatabase,
                owner,
                tableName,
                BASE_TABLE_TYPES)) {
            String resolvedOwner = null;
            int matches = 0;
            while (resultSet.next()) {
                if (!isBaseTable(resultSet.getString("TABLE_TYPE"))) {
                    continue;
                }
                String candidate = resultSet.getString("TABLE_NAME");
                if (!tableName.equals(candidate)) {
                    continue;
                }
                String candidateOwner = resultSet.getString("TABLE_SCHEM");
                if (hasText(owner)
                        && hasText(candidateOwner)
                        && !owner.equals(candidateOwner.trim())) {
                    continue;
                }
                matches++;
                resolvedOwner = hasText(candidateOwner)
                        ? candidateOwner.trim()
                        : owner;
            }

            if (matches == 0) {
                if (failWhenMissing) {
                    throw new TableNotFoundException(
                            catalogName,
                            TablePath.of(defaultDatabase, owner, tableName));
                }
                return null;
            }
            if (matches > 1 && !hasText(owner)) {
                throw new CatalogException(
                        "GBase 8s 表名在多个 owner 下存在，必须显式指定 owner.table："
                                + tableName);
            }
            return TablePath.of(defaultDatabase, resolvedOwner, tableName);
        }
    }

    private void requireCurrentDatabase(String databaseName) {
        if (hasText(databaseName)
                && !defaultDatabase.equalsIgnoreCase(databaseName.trim())) {
            throw new IllegalArgumentException(
                    "GBase 8s Stage 1 不支持跨 database table_path；当前 JDBC database="
                            + defaultDatabase
                            + "，请求 database="
                            + databaseName);
        }
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
            throw new CatalogException("找不到 GBase 8s JDBC Driver：" + config.getDriverClass(), e);
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("Catalog 尚未打开，请先调用 open()");
        }
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
