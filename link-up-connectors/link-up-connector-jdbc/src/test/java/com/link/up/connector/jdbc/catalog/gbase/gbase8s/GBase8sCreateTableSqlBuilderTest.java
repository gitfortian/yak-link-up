package com.link.up.connector.jdbc.catalog.gbase.gbase8s;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8s.GBase8sTypeMapper;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GBase8sCreateTableSqlBuilderTest {

    @Test
    public void buildsConservativeNativeCreateTableSql() {
        TableSchema schema = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("id", BasicType.LONG_TYPE)
                                .nullable(false)
                                .autoIncrement(true)
                                .defaultValue(1)
                                .comment("ignored")
                                .build(),
                        Column.builder("name", BasicType.STRING_TYPE)
                                .length(128L)
                                .nullable(true)
                                .defaultValue("anonymous")
                                .build(),
                        Column.builder("amount", new DecimalType(20, 4))
                                .nullable(false)
                                .build(),
                        Column.builder("payload", BasicType.BYTES_TYPE)
                                .nullable(true)
                                .build(),
                        Column.builder("created_at", BasicType.TIMESTAMP_TYPE)
                                .nullable(false)
                                .build()))
                .primaryKey(PrimaryKey.of(Arrays.asList("id")))
                .build();
        CatalogTable table = CatalogTable.builder(
                        TablePath.of("targetdb", "gbasedbt", "orders"),
                        schema)
                .comment("ignored table comment")
                .build();

        String sql = new GBase8sCreateTableSqlBuilder(
                table.getTablePath(),
                table,
                new GBase8sTypeMapper(),
                false)
                .build();

        assertTrue(sql.startsWith("CREATE TABLE gbasedbt.orders ("));
        assertTrue(sql.contains("id BIGINT NOT NULL"));
        assertTrue(sql.contains("name VARCHAR(128) NULL"));
        assertTrue(sql.contains("amount DECIMAL(20,4) NOT NULL"));
        assertTrue(sql.contains("payload BYTE NULL"));
        assertTrue(sql.contains("created_at DATETIME YEAR TO FRACTION(5) NOT NULL"));
        assertTrue(sql.contains("PRIMARY KEY (id)"));
        assertFalse(sql.contains("SERIAL"));
        assertFalse(sql.contains("DEFAULT"));
        assertFalse(sql.contains("COMMENT"));
    }

    @Test
    public void delimidentControlsQuotedCasePreservingIdentifiers() {
        TableSchema schema = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("OrderId", BasicType.INT_TYPE)
                                .nullable(false)
                                .build()))
                .build();
        CatalogTable table = CatalogTable.builder(
                        TablePath.of("targetdb", "MixedOwner", "Orders"),
                        schema)
                .build();

        String sql = new GBase8sCreateTableSqlBuilder(
                table.getTablePath(),
                table,
                new GBase8sTypeMapper(),
                true)
                .build();

        assertTrue(sql.contains("CREATE TABLE \"MixedOwner\".\"Orders\""));
        assertTrue(sql.contains("\"OrderId\" INTEGER NOT NULL"));
    }

    @Test
    public void defaultModeRejectsIdentifiersThatNeedDelimident() {
        TableSchema schema = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("order-id", BasicType.INT_TYPE).build()))
                .build();
        CatalogTable table = CatalogTable.builder(
                        TablePath.of("targetdb", "gbasedbt", "orders"),
                        schema)
                .build();

        assertThrows(
                IllegalArgumentException.class,
                () -> new GBase8sCreateTableSqlBuilder(
                        table.getTablePath(),
                        table,
                        new GBase8sTypeMapper(),
                        false)
                        .build());
    }

    @Test
    public void lobPrimaryKeyFailsBeforeExecutingUnsafeDdl() {
        TableSchema schema = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("external_id", BasicType.STRING_TYPE)
                                .nullable(false)
                                .build()))
                .primaryKey(PrimaryKey.of(Arrays.asList("external_id")))
                .build();
        CatalogTable table = CatalogTable.builder(
                        TablePath.of("targetdb", "gbasedbt", "orders"),
                        schema)
                .build();

        UnsupportedOperationException error = assertThrows(
                UnsupportedOperationException.class,
                () -> new GBase8sCreateTableSqlBuilder(
                        table.getTablePath(),
                        table,
                        new GBase8sTypeMapper(),
                        false)
                        .build());

        assertTrue(error.getMessage().contains("create_primary_key=false"));
        assertTrue(error.getMessage().contains("TEXT"));
    }
}
