package com.link.up.connector.clickhouse.client;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.connector.clickhouse.config.ClickHouseSourceConfig;
import com.link.up.connector.clickhouse.config.ClickHouseSourceTableConfig;
import com.link.up.connector.clickhouse.schema.ClickHouseTypeMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/** Metadata and planning client backed by the official ClickHouse JDBC/HTTP driver. */
public final class ClickHouseJdbcClient implements AutoCloseable {

    private static final String DRIVER_CLASS = "com.clickhouse.jdbc.ClickHouseDriver";

    private final ClickHouseSourceConfig config;

    public ClickHouseJdbcClient(ClickHouseSourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        ensureDriver();
    }

    public List<CatalogTable> discoverTables() throws SQLException {
        List<CatalogTable> result = new ArrayList<CatalogTable>();
        for (ClickHouseSourceTableConfig tableConfig : config.getTableConfigs()) {
            result.add(discoverTable(tableConfig));
        }
        return Collections.unmodifiableList(result);
    }

    public CatalogTable discoverTable(ClickHouseSourceTableConfig tableConfig)
            throws SQLException {
        Objects.requireNonNull(tableConfig, "tableConfig must not be null");
        TableSchema schema =
                tableConfig.isSqlMode()
                        ? discoverSqlSchema(tableConfig)
                        : discoverPhysicalTableSchema(tableConfig);
        return CatalogTable.builder(tableConfig.getTablePath(), schema)
                .option("connector", "clickhouse")
                .option("read_mode", tableConfig.isSqlMode() ? "sql" : "part")
                .build();
    }

    private TableSchema discoverPhysicalTableSchema(ClickHouseSourceTableConfig tableConfig)
            throws SQLException {
        String database = requireText(tableConfig.getDatabase(), "database");
        String table = requireText(tableConfig.getTable(), "table");
        String sql =
                "SELECT name, type, is_in_primary_key "
                        + "FROM system.columns "
                        + "WHERE database = ? AND table = ? "
                        + "ORDER BY position";

        TableSchema.Builder builder = TableSchema.builder();
        List<String> primaryKeys = new ArrayList<String>();
        int columnCount = 0;
        try (Connection connection = openConnection(config.getHosts().get(0), database);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, database);
            statement.setString(2, table);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String name = resultSet.getString(1);
                    String sourceType = resultSet.getString(2);
                    builder.column(ClickHouseTypeMapper.toColumn(name, sourceType));
                    if (resultSet.getInt(3) != 0) {
                        primaryKeys.add(name);
                    }
                    columnCount++;
                }
            }
        }
        if (columnCount == 0) {
            throw new SQLException(
                    "ClickHouse table has no discoverable columns or does not exist: "
                            + tableConfig.getTablePath());
        }
        if (!primaryKeys.isEmpty()) {
            builder.primaryKey(PrimaryKey.of("pk_" + String.join("_", primaryKeys), primaryKeys));
        }
        return builder.build();
    }

    private TableSchema discoverSqlSchema(ClickHouseSourceTableConfig tableConfig)
            throws SQLException {
        String query =
                "SELECT * FROM ("
                        + stripTrailingSemicolon(tableConfig.getSql())
                        + ") AS _link_up_schema LIMIT 0";
        String database =
                tableConfig.getDatabase() == null || tableConfig.isSyntheticTablePath()
                        ? "default"
                        : tableConfig.getDatabase();

        TableSchema.Builder builder = TableSchema.builder();
        try (Connection connection = openConnection(config.getHosts().get(0), database);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            ResultSetMetaData metadata = resultSet.getMetaData();
            if (metadata == null || metadata.getColumnCount() == 0) {
                throw new SQLException("ClickHouse sql query returned no column metadata");
            }
            for (int index = 1; index <= metadata.getColumnCount(); index++) {
                String name = metadata.getColumnLabel(index);
                if (name == null || name.trim().isEmpty()) {
                    name = metadata.getColumnName(index);
                }
                String sourceType = metadata.getColumnTypeName(index);
                Column column = ClickHouseTypeMapper.toColumn(name, sourceType);
                if (metadata.isNullable(index) != ResultSetMetaData.columnNoNulls
                        && !column.isNullable()) {
                    column = column.toBuilder().nullable(true).build();
                }
                builder.column(column);
            }
        }
        return builder.build();
    }

    public String getTableEngine(String endpoint, ClickHouseSourceTableConfig tableConfig)
            throws SQLException {
        String sql =
                "SELECT engine FROM system.tables WHERE database = ? AND name = ? LIMIT 1";
        try (Connection connection = openConnection(endpoint, tableConfig.getDatabase());
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tableConfig.getDatabase());
            statement.setString(2, tableConfig.getTable());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new SQLException(
                            "ClickHouse table does not exist on node "
                                    + endpoint
                                    + ": "
                                    + tableConfig.getTablePath());
                }
                return resultSet.getString(1);
            }
        }
    }

    public List<String> listActiveParts(
            String endpoint,
            ClickHouseSourceTableConfig tableConfig)
            throws SQLException {
        StringBuilder sql =
                new StringBuilder(
                        "SELECT name FROM system.parts "
                                + "WHERE database = ? AND table = ? AND active = 1");
        List<String> partitions = tableConfig.getPartitionList();
        if (!partitions.isEmpty()) {
            sql.append(" AND partition IN (");
            for (int i = 0; i < partitions.size(); i++) {
                if (i > 0) {
                    sql.append(',');
                }
                sql.append('?');
            }
            sql.append(')');
        }
        sql.append(" ORDER BY name");

        List<String> result = new ArrayList<String>();
        try (Connection connection = openConnection(endpoint, tableConfig.getDatabase());
             PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int parameter = 1;
            statement.setString(parameter++, tableConfig.getDatabase());
            statement.setString(parameter++, tableConfig.getTable());
            for (String partition : partitions) {
                statement.setString(parameter++, partition);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String part = resultSet.getString(1);
                    if (part != null && !part.trim().isEmpty()) {
                        result.add(part.trim());
                    }
                }
            }
        }
        return Collections.unmodifiableList(result);
    }

    public Connection openConnection(String endpoint, String database) throws SQLException {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : config.getClientConfig().entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                properties.setProperty(entry.getKey(), entry.getValue());
            }
        }
        properties.setProperty("user", config.getUsername());
        properties.setProperty("username", config.getUsername());
        properties.setProperty("password", config.getPassword());
        if (config.getServerTimeZone() != null) {
            properties.setProperty("server_time_zone", config.getServerTimeZone());
            properties.setProperty("use_server_time_zone", "true");
        }
        return DriverManager.getConnection(buildJdbcUrl(endpoint, database), properties);
    }

    public static String buildJdbcUrl(String endpoint, String database) {
        String node = requireText(endpoint, "endpoint");
        if (node.startsWith("jdbc:")) {
            throw new IllegalArgumentException(
                    "ClickHouse host must be an HTTP endpoint, not a JDBC URL: " + endpoint);
        }
        if (!node.startsWith("http://") && !node.startsWith("https://")) {
            node = "http://" + node;
        }
        while (node.endsWith("/")) {
            node = node.substring(0, node.length() - 1);
        }
        String targetDatabase =
                database == null || database.trim().isEmpty() ? "default" : database.trim();
        return "jdbc:clickhouse:" + node + "/" + targetDatabase;
    }

    public static String stripTrailingSemicolon(String sql) {
        String value = requireText(sql, "sql");
        while (value.endsWith(";")) {
            value = value.substring(0, value.length() - 1).trim();
        }
        return value;
    }

    private static void ensureDriver() {
        try {
            Class.forName(DRIVER_CLASS);
        } catch (ClassNotFoundException failure) {
            throw new IllegalStateException(
                    "ClickHouse JDBC driver is not available: " + DRIVER_CLASS,
                    failure);
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value.trim();
    }

    @Override
    public void close() {
        // Connections are task/planning scoped and closed at each call site.
    }
}
