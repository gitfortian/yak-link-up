package com.link.up.connector.jdbc.catalog;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.config.JdbcSourceConfig;
import com.link.up.connector.jdbc.config.JdbcSourceTableConfig;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.internal.JdbcConnectionProvider;
import com.link.up.connector.jdbc.options.MultiTableFailurePolicy;
import com.link.up.connector.jdbc.source.JdbcSourceTable;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Loads JDBC Source table metadata for the catalog/source planning boundary.
 *
 * <p>This class belongs to the catalog role: it translates Source table
 * configuration and JDBC metadata into {@link JdbcSourceTable} values. It does
 * not schedule tasks or own reader execution.</p>
 */
@Slf4j
public final class JdbcCatalogUtils {

    private JdbcCatalogUtils() {
    }

    public static Map<TablePath, JdbcSourceTable> getTables(
            JdbcSourceConfig config,
            JdbcDialect dialect)
            throws Exception {

        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(dialect, "dialect must not be null");

        try (Catalog catalog =
                     dialect.createCatalog(config.getConnectionConfig())) {
            catalog.open();
            return getTables(config, dialect, catalog);
        }
    }

    public static Map<TablePath, JdbcSourceTable> getTables(
            JdbcSourceConfig config,
            JdbcDialect dialect,
            Catalog catalog)
            throws Exception {

        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(dialect, "dialect must not be null");
        Objects.requireNonNull(catalog, "catalog must not be null");

        Map<TablePath, JdbcSourceTable> result = new LinkedHashMap<TablePath, JdbcSourceTable>();
        MultiTableFailurePolicy failurePolicy = config.getMultiTableFailurePolicy();

        for (JdbcSourceTableConfig tableConfig : config.getTableConfigs()) {
            try {
                JdbcSourceTable sourceTable =
                        loadSourceTable(config, tableConfig, dialect, catalog);

                JdbcSourceTable previous = result.put(sourceTable.getTablePath(), sourceTable);
                if (previous != null) {
                    throw new IllegalArgumentException(
                            "Source table path duplicated: " + sourceTable.getTablePath());
                }

                log.info(
                        "JDBC Source table metadata loaded, table={}, fields={}",
                        sourceTable.getTablePath(),
                        sourceTable.getCatalogTable().getTableSchema().getColumnCount());
            } catch (Exception e) {
                if (!shouldContinue(failurePolicy)) {
                    throw wrapException(tableConfig, e);
                }
                log.warn(
                        "Skipping failed JDBC Source table metadata, table={}, reason={}",
                        tableConfig.getTablePath(),
                        e.getMessage(),
                        e);
            }
        }

        if (result.isEmpty()) {
            throw new CatalogException("No JDBC Source table metadata was loaded successfully");
        }

        log.info(
                "JDBC Source table metadata loaded, success={}, configured={}",
                result.size(),
                config.getTableConfigs().size());

        return Collections.unmodifiableMap(result);
    }

    private static JdbcSourceTable loadSourceTable(
            JdbcSourceConfig sourceConfig,
            JdbcSourceTableConfig tableConfig,
            JdbcDialect dialect,
            Catalog catalog)
            throws Exception {

        TablePath tablePath = parseTablePath(tableConfig, dialect);
        CatalogTable catalogTable;

        if (tableConfig.hasCustomQuery()) {
            /*
             * Custom SQL describes a logical dataset, not necessarily one physical table.
             *
             * Yak Ops uses table_path (for example yak_query.<job>) as the stable
             * dataset id for routing. The query itself is the schema source, so do
             * not resolve the logical path through Catalog#getTable.
             */
            catalogTable =
                    discoverCatalogTableByQuery(
                            sourceConfig.getConnectionConfig(),
                            dialect,
                            tablePath,
                            tableConfig.getQuery());
        } else {
            catalogTable = catalog.getTable(tablePath);
        }

        return JdbcSourceTable.builder()
                .tablePath(tablePath)
                .query(tableConfig.getQuery())
                .partitionColumn(tableConfig.getPartitionColumn())
                .partitionNumber(tableConfig.getPartitionNumber())
                .partitionStart(tableConfig.getPartitionLowerBound())
                .partitionEnd(tableConfig.getPartitionUpperBound())
                .catalogTable(catalogTable)
                .build();
    }

    private static TablePath parseTablePath(
            JdbcSourceTableConfig tableConfig,
            JdbcDialect dialect) {

        String tablePath = tableConfig.getTablePath();
        if (!hasText(tablePath)) {
            throw new IllegalArgumentException("table_path must not be empty");
        }

        TablePath parsed = dialect.parseTablePath(tablePath);
        if (parsed == null) {
            throw new IllegalArgumentException("Cannot parse table_path: " + tablePath);
        }
        return parsed;
    }

    private static CatalogTable discoverCatalogTableByQuery(
            JdbcConnectionConfig connectionConfig,
            JdbcDialect dialect,
            TablePath logicalTablePath,
            String query)
            throws Exception {

        validateQuery(query);
        List<Column> queryColumns =
                readQueryColumns(connectionConfig, dialect, query);

        if (queryColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot discover custom query fields, table="
                            + logicalTablePath);
        }

        TableSchema querySchema =
                TableSchema.builder()
                        .columns(queryColumns)
                        .build();

        return CatalogTable.builder(
                        logicalTablePath,
                        querySchema)
                .build();
    }

    private static List<Column> readQueryColumns(
            JdbcConnectionConfig config,
            JdbcDialect dialect,
            String query)
            throws Exception {

        // Metadata discovery must use the same dialect-resolved connection
        // properties as the runtime reader. Otherwise database-specific session
        // settings (for example Xugu compatiblemode/current_schema) can differ
        // between planning and execution.
        try (JdbcConnectionProvider connectionProvider =
                     new JdbcConnectionProvider(config, dialect)) {
            Connection connection = connectionProvider.getOrEstablishConnection();
            try (PreparedStatement statement = connection.prepareStatement(query)) {

                ResultSetMetaData metadata = null;
                try {
                    metadata = statement.getMetaData();
                } catch (SQLException metadataError) {
                    log.debug(
                            "PreparedStatement metadata is unavailable; falling back to ResultSet metadata, sql={}",
                            abbreviate(query, 300),
                            metadataError);
                }

                if (metadata != null) {
                    return dialect.typeMapper().map(metadata);
                }

                if (query.indexOf('?') >= 0) {
                    throw new IllegalArgumentException(
                            "Custom query contains parameter placeholders; catalog phase cannot discover result schema: "
                                    + query);
                }

                statement.setMaxRows(1);
                try (ResultSet resultSet = statement.executeQuery()) {
                    return dialect.typeMapper().map(resultSet.getMetaData());
                }
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    "Failed to discover custom query schema, sql="
                            + abbreviate(query, 300),
                    e);
        }
    }

    private static void validateQuery(String query) {
        String sql = normalize(query);
        if (sql == null) {
            throw new IllegalArgumentException("query must not be empty");
        }

        String lower = sql.toLowerCase(Locale.ROOT);
        if (!lower.startsWith("select ")
                && !lower.startsWith("select\n")
                && !lower.startsWith("select\t")
                && !lower.startsWith("with ")
                && !lower.startsWith("with\n")
                && !lower.startsWith("with\t")) {
            throw new IllegalArgumentException(
                    "JDBC Source query only allows SELECT or WITH statements");
        }
    }

    private static boolean shouldContinue(
            MultiTableFailurePolicy policy) {
        return policy != null && policy.continueOtherTables();
    }

    private static RuntimeException wrapException(
            JdbcSourceTableConfig tableConfig,
            Exception error) {

        if (error instanceof RuntimeException) {
            return (RuntimeException) error;
        }

        return new CatalogException(
                "Failed to load JDBC Source table, table="
                        + tableConfig.getTablePath(),
                error);
    }

    private static String abbreviate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength) + "...";
    }

    private static boolean hasText(String value) {
        return normalize(value) != null;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim();
        return normalized.isEmpty() ? null : normalized;
    }
}
