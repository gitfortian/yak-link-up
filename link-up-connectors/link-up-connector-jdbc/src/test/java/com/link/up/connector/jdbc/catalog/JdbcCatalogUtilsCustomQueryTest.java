package com.link.up.connector.jdbc.catalog;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.catalog.exception.CatalogException;
import com.link.up.api.table.catalog.exception.TableNotFoundException;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.config.JdbcSourceConfig;
import com.link.up.connector.jdbc.core.converter.JdbcRowConverter;
import com.link.up.connector.jdbc.core.dialect.JdbcDialect;
import com.link.up.connector.jdbc.core.dialect.JdbcTypeMapper;
import com.link.up.connector.jdbc.core.dialect.sqlserver.SqlServerTypeMapper;
import com.link.up.connector.jdbc.source.JdbcSourceTable;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JdbcCatalogUtilsCustomQueryTest {

    private static final String URL =
            "jdbc:h2:mem:jdbc_custom_query;MODE=MSSQLServer;DB_CLOSE_DELAY=-1";

    private final JdbcDialect dialect = new TestDialect();
    private final Catalog catalog = new NoPhysicalLookupCatalog();

    @Before
    public void setUp() throws Exception {
        Class.forName("org.h2.Driver");
        try (Connection connection = DriverManager.getConnection(URL);
             Statement statement = connection.createStatement()) {
            statement.execute("DROP ALL OBJECTS");
            statement.execute(
                    "CREATE TABLE CBO_Supplier ("
                            + "ID BIGINT PRIMARY KEY, "
                            + "Code VARCHAR(64))");
            statement.execute(
                    "CREATE TABLE CBO_Supplier_Trl ("
                            + "ID BIGINT PRIMARY KEY, "
                            + "Name NVARCHAR(255))");
        }
    }

    @Test
    public void discoversSingleTableCustomQueryWithoutPhysicalLookup()
            throws Exception {

        JdbcSourceTable table =
                discover(
                        "yak_query.single_table",
                        "SELECT s.Code FROM CBO_Supplier s");

        assertLogicalPath(table, "single_table");
        assertColumnNames(
                table.getCatalogTable().getTableSchema(),
                "CODE");
    }

    @Test
    public void discoversJoinCustomQueryAcrossPhysicalTables()
            throws Exception {

        JdbcSourceTable table =
                discover(
                        "yak_query.join_table",
                        "SELECT s.Code, t.Name "
                                + "FROM CBO_Supplier s "
                                + "LEFT JOIN CBO_Supplier_Trl t ON s.ID = t.ID");

        assertLogicalPath(table, "join_table");
        assertColumnNames(
                table.getCatalogTable().getTableSchema(),
                "CODE",
                "NAME");
    }

    @Test
    public void discoversCustomQueryAliasesFromResultMetadata()
            throws Exception {

        JdbcSourceTable table =
                discover(
                        "yak_query.alias_table",
                        "SELECT s.Code AS supplier_code, "
                                + "t.Name AS supplier_name "
                                + "FROM CBO_Supplier s "
                                + "LEFT JOIN CBO_Supplier_Trl t ON s.ID = t.ID");

        assertLogicalPath(table, "alias_table");
        assertColumnNames(
                table.getCatalogTable().getTableSchema(),
                "SUPPLIER_CODE",
                "SUPPLIER_NAME");
    }

    private JdbcSourceTable discover(
            String logicalTablePath,
            String query)
            throws Exception {

        Map<String, Object> options =
                new LinkedHashMap<String, Object>();
        options.put("url", URL);
        options.put("driver", "org.h2.Driver");
        options.put("table_path", logicalTablePath);
        options.put("query", query);

        JdbcSourceConfig config =
                JdbcSourceConfig.of(
                        ReadonlyConfig.fromMap(options));

        Map<TablePath, JdbcSourceTable> tables =
                JdbcCatalogUtils.getTables(
                        config,
                        dialect,
                        catalog);

        assertEquals(1, tables.size());
        return tables.values().iterator().next();
    }

    private static void assertLogicalPath(
            JdbcSourceTable table,
            String expectedTableName) {

        assertEquals(
                "yak_query",
                table.getTablePath().getSchemaName());
        assertEquals(
                expectedTableName,
                table.getTablePath().getTableName());
    }

    private static void assertColumnNames(
            TableSchema schema,
            String... expectedNames) {

        assertEquals(
                expectedNames.length,
                schema.getColumnCount());

        for (int index = 0;
             index < expectedNames.length;
             index++) {

            assertEquals(
                    expectedNames[index],
                    schema.getColumn(index)
                            .getName()
                            .toUpperCase());
        }
    }

    private static final class TestDialect
            implements JdbcDialect {

        private final JdbcTypeMapper typeMapper =
                new SqlServerTypeMapper();

        @Override
        public String name() {
            return "test-sqlserver";
        }

        @Override
        public Catalog createCatalog(
                String catalogName,
                JdbcConnectionConfig connectionConfig) {

            throw new UnsupportedOperationException(
                    "Catalog creation is not used by this test");
        }

        @Override
        public JdbcTypeMapper typeMapper() {
            return typeMapper;
        }

        @Override
        public JdbcRowConverter rowConverter() {
            return null;
        }

        @Override
        public TablePath parseTablePath(
                String tablePath) {

            String[] parts =
                    tablePath.trim().split("\\.");

            if (parts.length == 2) {
                return TablePath.of(
                        null,
                        parts[0],
                        parts[1]);
            }

            return JdbcDialect.super.parseTablePath(
                    tablePath);
        }
    }

    private static final class NoPhysicalLookupCatalog
            implements Catalog {

        @Override
        public String name() {
            return "no-physical-lookup";
        }

        @Override
        public void open() throws CatalogException {
        }

        @Override
        public List<String> listDatabases()
                throws CatalogException {

            return Collections.emptyList();
        }

        @Override
        public List<TablePath> listTables(
                String databaseName,
                String schemaName)
                throws CatalogException {

            return Collections.emptyList();
        }

        @Override
        public boolean tableExists(
                TablePath tablePath)
                throws CatalogException {

            throw new AssertionError(
                    "Custom SQL must not inspect a physical table");
        }

        @Override
        public CatalogTable getTable(
                TablePath tablePath)
                throws CatalogException,
                TableNotFoundException {

            throw new AssertionError(
                    "Custom SQL must not call Catalog#getTable");
        }

        @Override
        public void close() throws CatalogException {
        }
    }
}
