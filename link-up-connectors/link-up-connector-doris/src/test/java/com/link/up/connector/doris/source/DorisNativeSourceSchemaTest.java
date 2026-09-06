package com.link.up.connector.doris.source;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.doris.config.DorisSourceTableConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class DorisNativeSourceSchemaTest {

    @Test
    public void projectsColumnsAndNormalizesLargeIntToString() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                Column.builder("id", new DecimalType(20, 0))
                                        .sourceType("LARGEINT")
                                        .build())
                        .column(Column.builder("name", BasicType.STRING_TYPE).sourceType("VARCHAR(32)").build())
                        .build();
        CatalogTable discovered = CatalogTable.builder(TablePath.of("db", "t"), schema).build();
        DorisSourceTableConfig config =
                new DorisSourceTableConfig(
                        "db", "t", "", Arrays.asList("id"), Integer.MAX_VALUE, 1024, 1024L);

        CatalogTable prepared = DorisNativeSourceSchema.prepare(discovered, config);

        assertEquals(1, prepared.getTableSchema().getColumnCount());
        assertEquals(SqlType.STRING, prepared.getTableSchema().getColumn(0).getDataType().getSqlType());
        assertEquals("native-thrift-arrow", prepared.getOptions().get("read_mode"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsArrayUntilDedicatedArrowComplexConversionExists() {
        TableSchema schema =
                TableSchema.builder()
                        .column(Column.builder("tags", BasicType.STRING_TYPE).sourceType("ARRAY<VARCHAR(20)>").build())
                        .build();
        CatalogTable discovered = CatalogTable.builder(TablePath.of("db", "t"), schema).build();
        DorisSourceTableConfig config =
                new DorisSourceTableConfig(
                        "db", "t", "", Collections.<String>emptyList(), Integer.MAX_VALUE, 1024, 1024L);
        DorisNativeSourceSchema.prepare(discovered, config);
    }
}
