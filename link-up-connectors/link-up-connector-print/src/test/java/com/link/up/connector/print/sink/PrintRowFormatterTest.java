package com.link.up.connector.print.sink;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxRow;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PrintRowFormatterTest {

    private final PrintRowFormatter formatter = new PrintRowFormatter();

    @Test
    public void shouldFormatSchemaLineWithColumnNamesAndTypes() {
        assertEquals(
                "schema: dataset=datagen_table columns=[id<BIGINT>, name<STRING>, price<DECIMAL(10,2)>]",
                formatter.formatSchema("datagen_table", table()));
    }

    @Test
    public void shouldFormatRowAsJsonWithDeclaredColumnNames() {
        FluxRow row = FluxRow.of(1L, "jared", null);

        String line = formatter.formatRow("datagen_table", "datagen#0", 7L, table(), row);

        assertEquals(
                "data: dataset=datagen_table split=datagen#0 row=7 "
                        + "{\"id\":1,\"name\":\"jared\",\"price\":null}",
                line);
    }

    @Test
    public void shouldRenderDecimalAsPlainString() {
        FluxRow row = FluxRow.of(1L, "jared", new BigDecimal("3.14"));

        String line = formatter.formatRow("datagen_table", "datagen#0", 1L, table(), row);

        assertTrue("Expected the decimal to render as a plain string: " + line,
                line.endsWith("{\"id\":1,\"name\":\"jared\",\"price\":\"3.14\"}"));
    }

    @Test
    public void shouldRenderDateAndTimestampAsIso8601() {
        CatalogTable table = CatalogTable.builder(
                        TablePath.of("datagen_table"),
                        TableSchema.builder()
                                .column(Column.builder("d", BasicType.DATE_TYPE).build())
                                .column(Column.builder("ts", BasicType.TIMESTAMP_TYPE).build())
                                .build())
                .build();
        FluxRow row = FluxRow.of(
                LocalDate.of(2024, 1, 31),
                LocalDateTime.of(2024, 1, 31, 9, 30, 5));

        String line = formatter.formatRow("datagen_table", "datagen#0", 1L, table, row);

        assertTrue("Expected ISO date/timestamp output: " + line,
                line.endsWith("{\"d\":\"2024-01-31\",\"ts\":\"2024-01-31T09:30:05\"}"));
    }

    @Test
    public void shouldEncodeBytesAsBase64() {
        CatalogTable table = singleColumnTable("payload", BasicType.BYTES_TYPE);
        FluxRow row = FluxRow.of((Object) "miIZj".getBytes());

        String line = formatter.formatRow("datagen_table", "datagen#0", 1L, table, row);

        assertEquals(
                "data: dataset=datagen_table split=datagen#0 row=1 {\"payload\":\"bWlJWmo=\"}",
                line);
    }

    @Test
    public void shouldRejectRowArityMismatch() {
        FluxRow row = FluxRow.of(1L);

        try {
            formatter.formatRow("datagen_table", "datagen#0", 1L, table(), row);
            fail("Expected an arity mismatch to be rejected");
        } catch (IllegalStateException failure) {
            assertTrue(failure.getMessage().contains("arity"));
        }
    }

    @Test
    public void shouldRejectValueThatDoesNotMatchDeclaredType() {
        FluxRow row = FluxRow.of("not-a-long", "jared", null);

        try {
            formatter.formatRow("datagen_table", "datagen#0", 1L, table(), row);
            fail("Expected a mistyped value to be rejected");
        } catch (IllegalStateException failure) {
            assertTrue(failure.getMessage().contains("does not match its declared type"));
        }
    }

    private static CatalogTable table() {
        return CatalogTable.builder(
                        TablePath.of("datagen_table"),
                        TableSchema.builder()
                                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                                .column(Column.builder("name", BasicType.STRING_TYPE).build())
                                .column(Column.builder("price", new DecimalType(10, 2)).build())
                                .build())
                .build();
    }

    private static CatalogTable singleColumnTable(String name, BasicType<?> dataType) {
        return CatalogTable.builder(
                        TablePath.of("datagen_table"),
                        TableSchema.builder()
                                .column(Column.builder(name, dataType).build())
                                .build())
                .build();
    }
}
