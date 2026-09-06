package com.link.up.connector.jdbc.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.jdbc.catalog.mysql.MySqlCatalog;
import com.link.up.connector.jdbc.config.JdbcConnectionConfig;
import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TiDbSinkSupportTest {

    @Test
    public void sourceDatabaseDoesNotLeakIntoTidbTarget() {
        TablePath target =
                TiDbSinkSupport.resolveTargetPath(
                        config(
                                "jdbc:mysql://127.0.0.1:4000/target_db",
                                null),
                        TablePath.of(
                                "source_db",
                                "orders"));

        assertEquals(
                TablePath.of(
                        "target_db",
                        "orders"),
                target);
    }

    @Test
    public void configuredSchemaActsAsTargetDatabaseWhenUrlIsUnqualified() {
        TablePath target =
                TiDbSinkSupport.resolveTargetPath(
                        config(
                                "jdbc:mysql://127.0.0.1:4000",
                                "configured_db"),
                        TablePath.of(
                                "source_db",
                                "orders"));

        assertEquals(
                TablePath.of(
                        "configured_db",
                        "orders"),
                target);
    }

    @Test
    public void createTableSqlUsesMysqlCompatibleDdl() {
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Collections.singletonList(
                                        Column.builder(
                                                        "id",
                                                        BasicType.LONG_TYPE)
                                                .nullable(false)
                                                .sourceType(
                                                        "bigint unsigned")
                                                .build()))
                        .build();

        CatalogTable table =
                CatalogTable.builder(
                                TablePath.of(
                                        "source_db",
                                        "orders"),
                                schema)
                        .option(
                                MySqlCatalog.TABLE_OPTION_DIALECT,
                                DatabaseIdentifier.TIDB)
                        .build();

        String sql =
                TiDbSinkSupport.resolveCreateTableSql(
                        config(
                                "jdbc:mysql://127.0.0.1:4000/target_db",
                                null),
                        table);

        assertTrue(
                sql.startsWith(
                        "CREATE TABLE `target_db`.`orders`"));
        assertTrue(sql.contains("bigint unsigned"));
    }

    private static JdbcConnectionConfig config(
            String url,
            String schema) {

        Map<String, Object> values =
                new LinkedHashMap<String, Object>();

        values.put("url", url);
        values.put(
                "driver",
                "com.mysql.cj.jdbc.Driver");
        values.put(
                "dialect",
                DatabaseIdentifier.TIDB);

        if (schema != null) {
            values.put("schema", schema);
        }

        return JdbcConnectionConfig.of(
                ReadonlyConfig.fromMap(values));
    }
}
