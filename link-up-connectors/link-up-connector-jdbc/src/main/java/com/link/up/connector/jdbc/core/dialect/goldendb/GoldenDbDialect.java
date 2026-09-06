package com.link.up.connector.jdbc.core.dialect.goldendb;

import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.jdbc.catalog.JdbcCatalogConfig;
import com.link.up.connector.jdbc.catalog.goldendb.GoldenDbCatalog;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.config.ReadConsistency;
import com.link.up.connector.jdbc.core.converter.JdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;
import com.link.up.connector.jdbc.core.dialect.mysql.MySqlDialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * GoldenDB bounded offline JDBC dialect.
 *
 * <p>Stage 1 keeps GoldenDB as a first-class database identity while reusing
 * the mature MySQL-compatible JDBC execution semantics: Connector/J, type
 * mapping, INSERT/UPSERT, cursor reads and hash splitting. GoldenDB-specific
 * CDC, native bulk loading and distributed-table DDL are outside this stage.</p>
 */
public final class GoldenDbDialect
        implements JdbcDialect {

    private final MySqlDialect mysqlDelegate;

    public GoldenDbDialect(
            JdbcConnectionConfig connectionConfig) {

        if (connectionConfig == null) {
            throw new IllegalArgumentException(
                    "connectionConfig must not be null");
        }

        this.mysqlDelegate =
                new MySqlDialect(connectionConfig);
    }

    @Override
    public String name() {
        return DatabaseIdentifier.GOLDENDB;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            JdbcConnectionConfig connectionConfig) {

        return new GoldenDbCatalog(
                catalogName,
                new JdbcCatalogConfig(
                        connectionConfig.getUrl(),
                        connectionConfig.getUsername(),
                        connectionConfig.getPassword(),
                        connectionConfig.getDriverName(),
                        connectionConfig.getProperties(),
                        false));
    }

    @Override
    public JdbcTypeMapper typeMapper() {
        return mysqlDelegate.typeMapper();
    }

    @Override
    public JdbcRowConverter rowConverter() {
        return new GoldenDbJdbcRowConverter();
    }

    @Override
    public Set<ReadConsistency> supportedReadConsistencies() {
        return mysqlDelegate.supportedReadConsistencies();
    }

    @Override
    public TablePath parseTablePath(String tablePath) {
        return mysqlDelegate.parseTablePath(tablePath);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return mysqlDelegate.quoteIdentifier(identifier);
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return mysqlDelegate.tableIdentifier(tablePath);
    }

    @Override
    public Optional<String> buildUpsertSql(
            TablePath tablePath,
            List<String> fieldNames,
            List<String> primaryKeys) {

        return mysqlDelegate.buildUpsertSql(
                tablePath,
                fieldNames,
                primaryKeys);
    }

    @Override
    public PreparedStatement prepareReadStatement(
            Connection connection,
            String sql,
            int fetchSize)
            throws SQLException {

        return mysqlDelegate.prepareReadStatement(
                connection,
                sql,
                fetchSize);
    }

    @Override
    public Map<String, String> defaultConnectionProperties() {
        return mysqlDelegate.defaultConnectionProperties();
    }

    @Override
    public Optional<String> buildHashPartitionPredicate(
            Column column,
            int bucket,
            int bucketCount) {

        return mysqlDelegate.buildHashPartitionPredicate(
                column,
                bucket,
                bucketCount);
    }
}
