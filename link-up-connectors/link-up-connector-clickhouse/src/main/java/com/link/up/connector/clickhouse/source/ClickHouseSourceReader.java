package com.link.up.connector.clickhouse.source;

import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.clickhouse.client.ClickHouseJdbcClient;
import com.link.up.connector.clickhouse.config.ClickHouseSourceConfig;
import com.link.up.connector.clickhouse.config.ClickHouseSourceTableConfig;
import com.link.up.connector.clickhouse.converter.ClickHouseResultSetRowConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Task-local bounded ClickHouse reader. */
public final class ClickHouseSourceReader
        implements SourceReader<FluxRow, ClickHouseSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseSourceReader.class);

    private final ClickHouseSourceConfig config;
    private final Map<TablePath, CatalogTable> tables;
    private final int frameworkBatchSize;
    private final ClickHouseJdbcClient client;

    private List<ClickHouseSourceSplit> splits = Collections.emptyList();
    private int splitIndex;
    private ClickHouseSourceSplit currentSplit;
    private ClickHouseSourceTableConfig currentTableConfig;
    private Connection currentConnection;
    private Statement currentStatement;
    private ResultSet currentResultSet;
    private ClickHouseResultSetRowConverter currentConverter;
    private boolean opened;
    private boolean finished;

    public ClickHouseSourceReader(
            ClickHouseSourceConfig config,
            Map<TablePath, CatalogTable> tables,
            int batchSize) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.tables = Objects.requireNonNull(tables, "tables must not be null");
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        this.frameworkBatchSize = batchSize;
        this.client = new ClickHouseJdbcClient(config);
    }

    @Override
    public void open(List<ClickHouseSourceSplit> splits) throws Exception {
        if (opened) {
            throw new IllegalStateException("ClickHouseSourceReader has already been opened");
        }
        if (splits == null) {
            throw new IllegalArgumentException("splits must not be null");
        }
        this.splits =
                Collections.unmodifiableList(new ArrayList<ClickHouseSourceSplit>(splits));
        this.splitIndex = 0;
        this.finished = false;
        this.opened = true;
    }

    @Override
    public void open() throws Exception {
        open(Collections.<ClickHouseSourceSplit>emptyList());
    }

    @Override
    public void openSplit(ClickHouseSourceSplit split) throws Exception {
        checkOpened();
        if (currentSplit != null) {
            throw new IllegalStateException("A ClickHouse split is already open");
        }
        finished = false;
        openCurrentSplit(Objects.requireNonNull(split, "split must not be null"));
    }

    @Override
    public void closeSplit() throws Exception {
        closeCurrentSplit();
    }

    @Override
    public RecordBatch<FluxRow> readBatch() throws Exception {
        checkOpened();
        if (finished) {
            return RecordBatch.endOfInput();
        }

        while (true) {
            if (currentSplit == null) {
                if (!openNextSplit()) {
                    finished = true;
                    return RecordBatch.endOfInput();
                }
            }

            ClickHouseSourceSplit batchSplit = currentSplit;
            int rowLimit = Math.min(frameworkBatchSize, currentTableConfig.getBatchSize());
            List<FluxRow> rows = new ArrayList<FluxRow>(rowLimit);
            boolean exhausted = false;
            while (rows.size() < rowLimit) {
                if (!currentResultSet.next()) {
                    exhausted = true;
                    break;
                }
                rows.add(currentConverter.read(currentResultSet));
            }

            if (exhausted) {
                closeCurrentSplit();
            }
            if (!rows.isEmpty()) {
                return RecordBatch.of(batchSplit, rows);
            }
        }
    }

    private boolean openNextSplit() throws Exception {
        if (splitIndex >= splits.size()) {
            return false;
        }
        openCurrentSplit(splits.get(splitIndex++));
        return true;
    }

    private void openCurrentSplit(ClickHouseSourceSplit split) throws Exception {
        CatalogTable table = tables.get(split.getTablePath());
        if (table == null) {
            throw new IllegalArgumentException(
                    "Cannot find schema for ClickHouse split table: " + split.getTablePath());
        }
        ClickHouseSourceTableConfig tableConfig = config.getTableConfig(split.getTablePath());
        String database =
                tableConfig.isSyntheticTablePath() || tableConfig.getDatabase() == null
                        ? "default"
                        : tableConfig.getDatabase();

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = client.openConnection(split.getEndpoint(), database);
            statement = connection.createStatement();
            try {
                statement.setFetchSize(tableConfig.getBatchSize());
            } catch (SQLException unsupported) {
                LOG.debug(
                        "ClickHouse JDBC driver ignored fetch size {}: {}",
                        tableConfig.getBatchSize(),
                        unsupported.getMessage());
            }
            String sql = ClickHouseSourceSqlBuilder.build(split, tableConfig, table);
            LOG.info(
                    "Opening ClickHouse bounded split: splitId={}, endpoint={}, mode={}",
                    split.splitId(),
                    split.getEndpoint(),
                    split.getMode());
            LOG.debug("ClickHouse split SQL: {}", sql);
            resultSet = statement.executeQuery(sql);

            currentSplit = split;
            currentTableConfig = tableConfig;
            currentConnection = connection;
            currentStatement = statement;
            currentResultSet = resultSet;
            currentConverter = new ClickHouseResultSetRowConverter(table.getTableSchema());
        } catch (Exception failure) {
            closeQuietly(resultSet);
            closeQuietly(statement);
            closeQuietly(connection);
            throw failure;
        }
    }

    private void closeCurrentSplit() throws Exception {
        Exception failure = null;
        failure = closeResource(currentResultSet, failure);
        failure = closeResource(currentStatement, failure);
        failure = closeResource(currentConnection, failure);

        currentResultSet = null;
        currentStatement = null;
        currentConnection = null;
        currentConverter = null;
        currentTableConfig = null;
        currentSplit = null;

        if (failure != null) {
            throw failure;
        }
    }

    private static Exception closeResource(AutoCloseable resource, Exception failure) {
        if (resource == null) {
            return failure;
        }
        try {
            resource.close();
        } catch (Exception closeFailure) {
            if (failure == null) {
                return closeFailure;
            }
            failure.addSuppressed(closeFailure);
        }
        return failure;
    }

    private static void closeQuietly(AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception ignored) {
            // Preserve the original open failure.
        }
    }

    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("ClickHouseSourceReader has not been opened");
        }
    }

    @Override
    public void close() throws Exception {
        if (!opened) {
            return;
        }
        Exception failure = null;
        try {
            closeCurrentSplit();
        } catch (Exception closeFailure) {
            failure = closeFailure;
        }
        try {
            client.close();
        } catch (Exception closeFailure) {
            if (failure == null) {
                failure = closeFailure;
            } else {
                failure.addSuppressed(closeFailure);
            }
        } finally {
            opened = false;
            finished = true;
            splits = Collections.emptyList();
        }
        if (failure != null) {
            throw failure;
        }
    }
}
