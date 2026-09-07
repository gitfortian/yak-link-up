package com.link.up.connector.jdbc.catalog.gbase.gbase8a;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8a.GBase8aTypeMapper;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GBase8aCreateTableSqlBuilderTest {

    @Test
    public void buildsSafeRandomDistributionBaselineWithoutCopyingSourcePhysicalSemantics() {
        Column id = Column.builder("id", BasicType.LONG_TYPE)
                .nullable(false)
                .autoIncrement(true)
                .defaultValue(1)
                .comment("source identity")
                .build();
        Column name = Column.builder("name", BasicType.STRING_TYPE)
                .length(255L)
                .nullable(true)
                .defaultValue("anonymous")
                .build();
        Column amount = Column.builder("amount", new DecimalType(18, 2))
                .nullable(true)
                .build();
        Column createdAt = Column.builder("created_at", BasicType.TIMESTAMP_TYPE)
                .nullable(false)
                .build();

        TableSchema schema = TableSchema.builder()
                .columns(Arrays.asList(id, name, amount, createdAt))
                .primaryKey(PrimaryKey.of(Collections.singletonList("id")))
                .build();
        CatalogTable table = CatalogTable.builder(
                        TablePath.of("archive", "orders"),
                        schema)
                .comment("source table comment")
                .build();

        String sql = new GBase8aCreateTableSqlBuilder(
                table.getTablePath(),
                table,
                new GBase8aTypeMapper())
                .build();

        assertEquals(
                "CREATE TABLE `archive`.`orders` (\n"
                        + "    `id` BIGINT NOT NULL,\n"
                        + "    `name` VARCHAR(255) NULL,\n"
                        + "    `amount` DECIMAL(18,2) NULL,\n"
                        + "    `created_at` DATETIME NOT NULL,\n"
                        + "    PRIMARY KEY (`id`)\n"
                        + ");",
                sql);

        assertFalse(sql.contains("AUTO_INCREMENT"));
        assertFalse(sql.contains("DEFAULT"));
        assertFalse(sql.contains("COMMENT"));
        assertFalse(sql.contains("DISTRIBUTED"));
        assertFalse(sql.contains("REPLICATED"));
    }

    @Test
    public void usesLongTypesForUnknownOrLargePayloads() {
        TableSchema schema = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("text_payload", BasicType.STRING_TYPE).build(),
                        Column.builder("binary_payload", BasicType.BYTES_TYPE).build()))
                .build();
        CatalogTable table = CatalogTable.builder(
                        TablePath.of("archive", "payloads"),
                        schema)
                .build();

        String sql = new GBase8aCreateTableSqlBuilder(
                table.getTablePath(),
                table,
                new GBase8aTypeMapper())
                .build();

        assertTrue(sql.contains("`text_payload` LONGTEXT NULL"));
        assertTrue(sql.contains("`binary_payload` LONGBLOB NULL"));
    }
}
