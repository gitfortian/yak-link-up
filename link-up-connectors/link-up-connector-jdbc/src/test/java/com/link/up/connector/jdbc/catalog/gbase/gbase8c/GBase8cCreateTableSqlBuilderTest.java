package com.link.up.connector.jdbc.catalog.gbase.gbase8c;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cCompatibilityMode;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GBase8cCreateTableSqlBuilderTest {

    @Test
    public void pgModeBuildsConservativePortableTable() {
        String sql = builder(
                GBase8cCompatibilityMode.PG,
                tableWithPrimaryKey())
                .build();

        assertTrue(sql.startsWith("CREATE TABLE \"landing\".\"orders\" ("));
        assertTrue(sql.contains("\"id\" BIGINT NOT NULL"));
        assertTrue(sql.contains("\"name\" VARCHAR(128) NULL"));
        assertTrue(sql.contains("\"amount\" NUMERIC(18,2) NULL"));
        assertTrue(sql.contains("\"business_date\" DATE NOT NULL"));
        assertTrue(sql.contains("PRIMARY KEY (\"id\")"));
        assertSafeBaseline(sql);
    }

    @Test
    public void aModeUsesTextAndPhysicalDateContract() {
        String sql = builder(
                GBase8cCompatibilityMode.A,
                tableWithPrimaryKey())
                .build();

        assertTrue(sql.contains("\"name\" TEXT NULL"));
        assertTrue(sql.contains(
                "\"business_date\" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL"));
        assertSafeBaseline(sql);
    }

    @Test
    public void bAndCModeKeepDateButUseSafeTextBoundary() {
        String bSql = builder(GBase8cCompatibilityMode.B, tableWithPrimaryKey()).build();
        String cSql = builder(GBase8cCompatibilityMode.C, tableWithPrimaryKey()).build();

        assertTrue(bSql.contains("\"name\" TEXT NULL"));
        assertTrue(bSql.contains("\"business_date\" DATE NOT NULL"));
        assertTrue(cSql.contains("\"name\" TEXT NULL"));
        assertTrue(cSql.contains("\"business_date\" DATE NOT NULL"));
    }

    @Test
    public void sourceDefaultsIdentityAndCommentsAreNeverCopied() {
        Column id = Column.builder("id", BasicType.LONG_TYPE)
                .nullable(false)
                .autoIncrement(true)
                .defaultValue("nextval('source_seq')")
                .comment("source id")
                .build();
        TableSchema schema = TableSchema.builder()
                .columns(Collections.singletonList(id))
                .build();
        CatalogTable table = CatalogTable.builder(
                        TablePath.of("targetdb", "landing", "orders"),
                        schema)
                .build();

        String sql = builder(GBase8cCompatibilityMode.PG, table).build();
        assertFalse(sql.toLowerCase().contains("nextval"));
        assertFalse(sql.toLowerCase().contains("serial"));
        assertFalse(sql.toLowerCase().contains("default"));
        assertFalse(sql.toLowerCase().contains("comment"));
    }

    private static GBase8cCreateTableSqlBuilder builder(
            GBase8cCompatibilityMode mode,
            CatalogTable table) {
        return new GBase8cCreateTableSqlBuilder(
                TablePath.of("targetdb", "landing", "orders"),
                table,
                mode);
    }

    private static CatalogTable tableWithPrimaryKey() {
        Column id = Column.builder("id", BasicType.LONG_TYPE)
                .nullable(false)
                .build();
        Column name = Column.builder("name", BasicType.STRING_TYPE)
                .length(128L)
                .nullable(true)
                .build();
        Column amount = Column.builder("amount", new DecimalType(18, 2))
                .precision(18)
                .scale(2)
                .nullable(true)
                .build();
        Column businessDate = Column.builder("business_date", BasicType.DATE_TYPE)
                .nullable(false)
                .build();
        TableSchema schema = TableSchema.builder()
                .columns(Arrays.asList(id, name, amount, businessDate))
                .primaryKey(PrimaryKey.of(Collections.singletonList("id")))
                .build();
        return CatalogTable.builder(
                        TablePath.of("targetdb", "landing", "orders"),
                        schema)
                .build();
    }

    private static void assertSafeBaseline(String sql) {
        String lower = sql.toLowerCase();
        assertFalse(lower.contains("default"));
        assertFalse(lower.contains("auto_increment"));
        assertFalse(lower.contains("serial"));
        assertFalse(lower.contains("comment"));
        assertFalse(lower.contains("partition by"));
        assertFalse(lower.contains("distribute by"));
        assertFalse(lower.contains("replication"));
    }
}
