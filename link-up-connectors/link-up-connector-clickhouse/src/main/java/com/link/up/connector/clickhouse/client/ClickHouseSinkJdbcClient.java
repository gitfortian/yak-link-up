package com.link.up.connector.clickhouse.client;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.clickhouse.config.ClickHouseSinkConfig;
import com.link.up.connector.clickhouse.converter.ClickHousePreparedStatementBinder;
import com.link.up.connector.clickhouse.schema.ClickHouseTypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseSinkJdbcClient.class);
    private static final String DRIVER_CLASS = "com.clickhouse.jdbc.ClickHouseDriver";

    private final ClickHouseSinkConfig config;
    private final CatalogTable targetTable;
    private final TableSchema sourceSchema;
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
        this.sourceSchema = Objects.requireNonNull(sourceSchema, "sourceSchema must not be null");
        this.binder = new ClickHousePreparedStatementBinder(this.sourceSchema);
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
        insertStatement = connection.prepareStatement(buildInsertSql(targetTable, sourceSchema));
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

        // executeBatch has already crossed the remote durability boundary. Cleanup must not turn
        // a successful insert into a retryable task failure.
        pendingRows = 0;
        try {
            insertStatement.clearBatch();
        } catch (Exception cleanupFailure) {
            LOG.warn(
                    "ClickHouse executeBatch succeeded but clearBatch cleanup failed; the batch is already committed remotely",
                    cleanupFailure);
        }
        return rows;
    }

    public void clearBatch() throws Exception {
        if (insertStatement != null) {
            insertStatement.clearBatch();
        }
        pendingRows = 0;
    }

    static String buildInsertSql(CatalogTable targetTable, TableSchema sourceSchema) {
        TableSchema targetSchema = targetTable.getTableSchema();
        if (targetSchema.getColumnCount() != sourceSchema.getColumnCount()) {
            throw new IllegalArgumentException(
                    "Prepared ClickHouse source/target column counts differ");
        }

        StringBuilder targetColumns = new StringBuilder();
        StringBuilder selectColumns = new StringBuilder();
        StringBuilder inputSchema = new StringBuilder();
        for (int index = 0; index < targetSchema.getColumnCount(); index++) {
            if (index > 0) {
                targetColumns.append(", ");
                selectColumns.append(", ");
                inputSchema.append(", ");
            }
            Column targetColumn = targetSchema.getColumn(index);
            Column sourceColumn = sourceSchema.getColumn(index);
            String targetSourceType = targetColumn.getSourceType();
            if (targetSourceType == null || targetSourceType.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Prepared ClickHouse target column is missing sourceType: "
                                + targetColumn.getName());
            }
            String inputName = "c" + index;
            targetColumns.append(quoteIdentifier(targetColumn.getName()));
            selectColumns
                    .append("CAST(")
                    .append(inputName)
                    .append(" AS ")
                    .append(targetSourceType.trim())
                    .append(')');
            inputSchema.append(inputName).append(' ').append(inputType(sourceColumn));
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

    static String inputType(Column sourceColumn) {
        FluxDataType<?> dataType = sourceColumn.getDataType();
        SqlType sqlType = dataType.getSqlType();
        String type;
        switch (sqlType) {
            case BOOLEAN:
                type = "UInt8";
                break;
            case TINYINT:
                type = "Int8";
                break;
            case SMALLINT:
                type = "Int16";
                break;
            case INT:
                type = "Int32";
                break;
            case BIGINT:
                type = "Int64";
                break;
            case FLOAT:
                type = "Float32";
                break;
            case DOUBLE:
                type = "Float64";
                break;
            case DECIMAL:
                if (!(dataType instanceof DecimalType)) {
                    throw new IllegalArgumentException(
                            "ClickHouse Sink DECIMAL source is missing precision/scale metadata: "
                                    + sourceColumn.getName());
                }
                DecimalType decimal = (DecimalType) dataType;
                type = "Decimal(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
                break;
            case STRING:
                type = "String";
                break;
            case DATE:
                type = "Date32";
                break;
            case TIMESTAMP:
                type = "DateTime64(9)";
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported ClickHouse Sink input type for column "
                                + sourceColumn.getName()
                                + ": "
                                + sqlType);
        }
        return sourceColumn.isNullable() ? "Nullable(" + type + ")" : type;
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
            } catch (Exception cleanupFailure) {
                LOG.debug("Failed to discard an unflushed ClickHouse batch during close", cleanupFailure);
            }
            try {
                insertStatement.close();
            } catch (Exception closeFailure) {
                failure = closeFailure;
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
