package com.link.up.connector.print.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.print.config.PrintSinkConfig;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PrintSinkWriterTest {

    @Test
    public void shouldLogSchemaOncePerDataset() {
        List<String> lines = new ArrayList<String>();
        PrintSinkWriter writer = new PrintSinkWriter(PrintSinkConfig.of(ReadonlyConfig.fromMap(
                new LinkedHashMap<String, Object>())), capture(lines));

        writer.write(batch(2L), table());
        writer.write(batch(2L), table());

        assertEquals(5, lines.size());
        assertEquals(1, countSchemaLines(lines));
        assertTrue(lines.get(0).startsWith("schema: dataset=datagen_table "));
        assertTrue(lines.get(1).startsWith("data: "));
        assertTrue(lines.get(4).startsWith("data: "));
        writer.close();
    }

    private static int countSchemaLines(List<String> lines) {
        int count = 0;
        for (String line : lines) {
            if (line.startsWith("schema: ")) {
                count++;
            }
        }
        return count;
    }

    @Test
    public void shouldNumberRowsPerWriterFromOne() {
        List<String> lines = new ArrayList<String>();
        PrintSinkWriter writer = new PrintSinkWriter(PrintSinkConfig.of(ReadonlyConfig.fromMap(
                new LinkedHashMap<String, Object>())), capture(lines));

        writer.write(batch(2L), table());
        writer.write(batch(1L), table());

        assertTrue(lines.get(1).contains("row=1 "));
        assertTrue(lines.get(2).contains("row=2 "));
        assertTrue(lines.get(3).contains("row=3 "));
        writer.close();
    }

    @Test
    public void shouldNotLogRowsWhenPrintRowsDisabled() {
        List<String> lines = new ArrayList<String>();
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("print_rows", false);
        PrintSinkWriter writer = new PrintSinkWriter(
                PrintSinkConfig.of(ReadonlyConfig.fromMap(values)), capture(lines));

        writer.write(batch(3L), table());

        assertEquals(1, lines.size());
        assertTrue(lines.get(0).startsWith("schema: "));
        writer.close();
    }

    @Test
    public void shouldStopOnInterruptDuringInterval() throws Exception {
        List<String> lines = new ArrayList<String>();
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("print_interval_millis", 100L);
        PrintSinkWriter writer = new PrintSinkWriter(
                PrintSinkConfig.of(ReadonlyConfig.fromMap(values)), capture(lines));

        writer.write(batch(1L), table());
        Thread.currentThread().interrupt();
        try {
            writer.write(batch(1L), table());
            fail("Expected an interrupted print interval to stop the writer");
        } catch (IllegalStateException failure) {
            assertTrue(failure.getCause() instanceof InterruptedException);
            assertTrue("The interrupt flag must be restored", Thread.interrupted());
        } finally {
            Thread.interrupted();
            writer.close();
        }
    }

    @Test
    public void shouldRejectWriteAfterClose() {
        PrintSinkWriter writer = new PrintSinkWriter(PrintSinkConfig.of(ReadonlyConfig.fromMap(
                new LinkedHashMap<String, Object>())), capture(new ArrayList<String>()));
        writer.close();

        try {
            writer.write(batch(1L), table());
            fail("Expected write after close to be rejected");
        } catch (IllegalStateException failure) {
            assertTrue(failure.getMessage().contains("closed"));
        }
    }

    private static PrintSinkWriter.LineEmitter capture(final List<String> lines) {
        return new PrintSinkWriter.LineEmitter() {
            @Override
            public void emit(String line) {
                lines.add(line);
            }
        };
    }

    private static RecordBatch<FluxRow> batch(long rows) {
        FluxRow[] values = new FluxRow[(int) rows];
        for (int i = 0; i < rows; i++) {
            values[i] = FluxRow.of(1L, "jared", null);
        }
        return RecordBatch.of(
                "datagen_table",
                "datagen#0",
                Arrays.asList(values));
    }

    private static CatalogTable table() {
        return CatalogTable.builder(
                        TablePath.of("datagen_table"),
                        TableSchema.builder()
                                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                                .column(Column.builder("name", BasicType.STRING_TYPE).build())
                                .column(Column.builder("price", BasicType.LONG_TYPE).build())
                                .build())
                .build();
    }
}
