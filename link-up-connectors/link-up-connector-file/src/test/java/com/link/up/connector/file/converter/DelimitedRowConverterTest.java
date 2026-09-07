package com.link.up.connector.file.converter;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxRow;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DelimitedRowConverterTest {

    @Test
    public void shouldConvertDeclaredTypes() {
        FileRowConverter converter = converter(Collections.emptyList(), null);

        FluxRow row = converter.convert("1,3.14,widget,true", "test:1");

        assertEquals(1L, row.getField(0));
        assertEquals(new java.math.BigDecimal("3.14"), row.getField(1));
        assertEquals("widget", row.getField(2));
        assertEquals(Boolean.TRUE, row.getField(3));
    }

    @Test
    public void shouldFailFastOnUnsafeNumericText() {
        FileRowConverter converter = converter(Collections.emptyList(), null);

        try {
            converter.convert("x,3.14,widget,true", "in rows.csv at split offset 0, split-local line 3");
            fail("Expected a non-numeric id to fail fast");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("Column 'id'"));
            assertTrue(failure.getMessage().contains("split-local line 3"));
        }
    }

    @Test
    public void shouldRejectWrongFieldCount() {
        FileRowConverter converter = converter(Collections.emptyList(), null);

        try {
            converter.convert("1,3.14,widget", "test:1");
            fail("Expected a short row to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("fields"));
        }
    }

    @Test
    public void shouldMapNullValueTextToNull() {
        FileRowConverter converter = converter(Collections.emptyList(), "\\N");

        FluxRow row = converter.convert("1,\\N,widget,\\N", "test:1");

        assertEquals(1L, row.getField(0));
        assertEquals(null, row.getField(1));
    }

    @Test
    public void shouldProjectSelectedFields() {
        FileRowConverter converter = converter(Arrays.asList("price", "name"), null);

        FluxRow row = converter.convert("1,3.14,widget,true", "test:1");

        assertEquals(2, row.getArity());
        assertEquals(new java.math.BigDecimal("3.14"), row.getField(0));
        assertEquals("widget", row.getField(1));
    }

    @Test
    public void shouldKeepQuotedCommaInsideCsvField() {
        FileRowConverter converter = converter(Collections.emptyList(), null);

        FluxRow row = converter.convert("1,3.14,\"widget, inc\",true", "test:1");

        assertEquals("widget, inc", row.getField(2));
    }

    private FileRowConverter converter(List<String> fields, String nullValue) {
        TableSchema schema = TableSchema.builder()
                .column(Column.builder("id", BasicType.LONG_TYPE).nullable(true).build())
                .column(Column.builder("price", new DecimalType(10, 2)).nullable(true).build())
                .column(Column.builder("name", BasicType.STRING_TYPE).nullable(true).build())
                .column(Column.builder("flag", BasicType.BOOLEAN_TYPE).nullable(true).build())
                .build();
        return DelimitedRowConverter.csv(
                schema,
                DelimitedRowConverter.outputIndexes(schema, resolveProjected(schema, fields)),
                nullValue,
                '"',
                '"');
    }

    private static java.util.List<String> resolveProjected(
            TableSchema schema,
            List<String> fields) {

        if (fields == null || fields.isEmpty()) {
            java.util.List<String> names = new java.util.ArrayList<String>();
            for (int i = 0; i < schema.getColumnCount(); i++) {
                names.add(schema.getColumn(i).getName());
            }
            return names;
        }
        return fields;
    }
}
