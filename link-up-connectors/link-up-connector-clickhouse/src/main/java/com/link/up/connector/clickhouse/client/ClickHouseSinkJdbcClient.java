package com.link.up.connector.clickhouse.client;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.clickhouse.config.ClickHouseSinkConfig;
import com.link.up.connector.clickhouse.converter.ClickHousePreparedStatementBinder;
import com.link.up.connector.clickhouse.schema.ClickHouseTypeMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/** JDBC client for bounded ClickHouse target validation and prepared batch inserts. */
public final class ClickHouseSinkJdbcClient implements AutoCloseable {

    private static final String DRIVER_CLASS = "com.clickhouse.jdbc.ClickHouseDriver";

    private final ClickHouseSinkConfig config;
    private final CatalogTable targetTable;
    private final ClickHousePreparedStatementBinder binder;

    private Connection connection;
    private PreparedStatement insertStatement;
    private int pendingRows;

    public ClickHouseSinkJdbcClient(
            ClickHouseSinkConfig config,
            CatalogTable targetTable,
            TableSchema sourceSchema) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.targetTable = Objects.requireNonNull(targetTable, "targetTable must not be null");
        this.binder = new ClickHousePreparedStatementBinder(
                Objects.requireNonNull(sourceSchema, "sourceSchema must not be null"));
        ensureDriver();
    }

    public static CatalogTable discoverTargetTable(ClickHouseSinkConfig config) throws Exception {
        Objects.requireNonNull(config, "config must not be null");
        ensureDriver();
        String sql =
                "SELECT name, type, is_in_primary_key, default_kind "
                        + "FROM system.columns "
                        + "WHERE database = ? AND table = ? "
                        + "ORDER BY position";

        TableSchema.Builder builder = TableSchema.builder();
        List<String> primaryKeys = new ArrayList<String>();
        int writableColumns = 0;
        try (Connection connection = openConnection(config);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, config.getDatabase());
            statement.setString(2, config.getTable());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String name = resultSet.getString(1);
                    String sourceType = resultSet.getString(2);
                    int primaryKey = resultSet.getInt(3);
                    String defaultKind = resultSet.getString(4);
                    if (!isWritableColumn(defaultKind)) {
                        continue;
                    }
                    builder.column(ClickHouseTypeMapper.toColumn(name, sourceType));
                    if (primaryKey != 0) {
                        primaryKeys.add(name);
                    }
                    writableColumns++;
                }
            }
        }
        if (writableColumns == 0) {
            throw new IllegalArgumentException(
                    "ClickHouse sink target table does not exist or has no writable columns: "
                            + config.getDatabase()
                            + "."
                            + config.getTable());
        }
        if (!primaryKeys.isEmpty()) {
            builder.primaryKey(PrimaryKey.of("pk_" + String.join("_", primaryKeys), primaryKeys));
        }
        return CatalogTable.builder(config.getTargetPath(), builder.build())
                .option("connector", "clickhouse")
                .option("write_mode", "bounded-jdbc-batch")
                .build();
    }

    public void open() throws Exception {
        if (connection != null) {
            throw new IllegalStateException("ClickHouse sink JDBC client is already open");
        }
        connection = openConnection(config);
        insertStatement = connection.prepareStatement(buildInsertSql(targetTable));
        pendingRows = 0;
    }

    public void add(FluxRow row) throws Exception {
        checkOpen();
        binder.bind(insertStatement, row);
        insertStatement.addBatch();
        pendingRows++;
    }

    public int flush() throws Exception {
        checkOpen();
        if (pendingRows == 0) {
            return 0;
        }
        int rows = pendingRows;
        int[] results = insertStatement.executeBatch();
        if (results != null) {
            for (int result : results) {
                if (result == Statement.EXECUTE_FAILED) {
                    throw new IllegalStateException(
                            "ClickHouse JDBC executeBatch reported EXECUTE_FAILED; the batch outcome may be partial or ambiguous and is not retried automatically");
                }
            }
        }
        insertStatement.clearBatch();
        pendingRows = 0;
        return rows;
    }

    public void clearBatch() throws Exception {
        if (insertStatement != null) {
            insertStatement.clearBatch();
        }
        pendingRows = 0;
    }

    static String buildInsertSql(CatalogTable targetTable) {
        TableSchema schema = targetTable.getTableSchema();
        StringBuilder targetColumns = new StringBuilder();
        StringBuilder selectColumns = new StringBuilder();
        StringBuilder inputSchema = new StringBuilder();
        for (int index = 0; index < schema.getColumnCount(); index++) {
            if (index > 0) {
                targetColumns.append(", ");
                selectColumns.append(", ");
                inputSchema.append(", ");
            }
            Column column = schema.getColumn(index);
            String inputName = "c" + index;
            String sourceType = column.getSourceType();
            if (sourceType == null || sourceType.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "ClickHouse prepared target column is missing sourceType: " + column.getName());
            }
            targetColumns.append(quoteIdentifier(column.getName()));
            selectColumns.append(inputName);
            inputSchema.append(inputName).append(' ').append(sourceType.trim());
        }
        TablePath path = targetTable.getTablePath();
        return "INSERT INTO "
                + quoteIdentifier(path.getDatabaseName())
                + "."
                + quoteIdentifier(path.getTableName())
                + " ("
                + targetColumns
                + ") SELECT "
                + selectColumns
                + " FROM input('"
                + escapeStringLiteral(inputSchema.toString())
                + "')";
    }

    static Connection openConnection(ClickHouseSinkConfig config) throws Exception {
        String url = buildSafeJdbcUrl(config);
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : config.getClientConfig().entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        properties.setProperty("user", config.getUsername());
        properties.setProperty("username", config.getUsername());
        properties.setProperty("password", config.getPassword());
        if (config.getServerTimeZone() != null) {
            properties.setProperty("server_time_zone", config.getServerTimeZone());
            properties.setProperty("use_server_time_zone", "true");
        }
        // Keep the bounded writer's durability boundary stable even when ClickHouse 26.3+
        // enables async inserts by default at server/user level.
        properties.setProperty("async_insert", "0");
        properties.setProperty("wait_for_async_insert", "1");
        return DriverManager.getConnection(url, properties);
    }

    static String buildSafeJdbcUrl(ClickHouseSinkConfig config) {
        String base = ClickHouseJdbcClient.buildJdbcUrl(config.getHost(), config.getDatabase());
        return base + "?async_insert=0&wait_for_async_insert=1";
    }

    static boolean isWritableColumn(String defaultKind) {
        if (defaultKind == null || defaultKind.trim().isEmpty()) {
            return true;
        }
        String kind = defaultKind.trim().toUpperCase(Locale.ROOT);
        return !"MATERIALIZED".equals(kind)
                && !"ALIAS".equals(kind)
                && !"EPHEMERAL".equals(kind);
    }

    private static String quoteIdentifier(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("ClickHouse identifier must not be empty");
        }
        return "`" + value.replace("`", "``") + "`";
    }

    private static String escapeStringLiteral(String value) {
        return value.replace("\\", "\\\\").replace("'", "\\'");
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

    private void checkOpen() {
        if (insertStatement == null || connection == null) {
            throw new IllegalStateException("ClickHouse sink JDBC client is not open");
        }
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        if (insertStatement != null) {
            try {
                insertStatement.clearBatch();
            } catch (Exception clearFailure) {
                failure = clearFailure;
            }
            try {
                insertStatement.close();
            } catch (Exception closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        insertStatement = null;
        connection = null;
        pendingRows = 0;
        if (failure != null) {
            throw failure;
        }
    }
}
