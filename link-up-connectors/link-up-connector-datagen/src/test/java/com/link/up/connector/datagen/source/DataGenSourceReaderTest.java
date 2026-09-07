package com.link.up.connector.datagen.source;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.datagen.config.DataGenSourceConfig;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataGenSourceReaderTest {

    @Test
    public void shouldReturnEndOfInputAfterAllSplits() {
        DataGenSourceReader reader = reader(6L, null, 4);

        reader.open(new DataGenSourceSplitEnumerator(readerConfig(6L, null), 3).enumerateSplits());

        long totalRows = 0L;
        RecordBatch<FluxRow> batch;
        while (!(batch = reader.readBatch()).isEndOfInput()) {
            totalRows += batch.size();
            assertEquals("datagen_table", batch.getDataSetId());
        }
        assertEquals(6L, totalRows);
        reader.close();
    }

    @Test
    public void shouldGenerateContinuousGlobalRowsAcrossSplits() {
        DataGenSourceConfig config = readerConfig(4L, 2);
        DataGenSourceReader reader = new DataGenSourceReader(config, 10);
        reader.open(new DataGenSourceSplitEnumerator(config, 3).enumerateSplits());

        long expectedSequence = 1000L;
        RecordBatch<FluxRow> batch;
        while (!(batch = reader.readBatch()).isEndOfInput()) {
            for (FluxRow row : batch.getRecords()) {
                assertEquals(expectedSequence++, row.getField(0));
            }
        }
        assertEquals(1004L, expectedSequence);
        reader.close();
    }

    @Test
    public void shouldRespectReadIntervalWithoutBusyWait() {
        DataGenSourceReader reader = reader(8L, null, 2);

        reader.open(new DataGenSourceSplitEnumerator(readerConfig(8L, null), 3).enumerateSplits());

        reader.readBatch();
        long startMillis = System.currentTimeMillis();
        reader.readBatch();
        long elapsedMillis = System.currentTimeMillis() - startMillis;

        assertTrue(
                "Expected the read interval to delay the second batch, elapsed=" + elapsedMillis,
                elapsedMillis >= 90L);
        reader.close();
    }

    @Test
    public void shouldStopOnInterruptDuringInterval() {
        DataGenSourceReader reader = reader(8L, null, 2);

        reader.open(new DataGenSourceSplitEnumerator(readerConfig(8L, null), 3).enumerateSplits());
        reader.readBatch();

        Thread.currentThread().interrupt();
        try {
            reader.readBatch();
            fail("Expected an interrupted read interval to stop the reader");
        } catch (IllegalStateException failure) {
            assertTrue(failure.getCause() instanceof InterruptedException);
            assertTrue("The interrupt flag must be restored", Thread.interrupted());
        } finally {
            Thread.interrupted();
            reader.close();
        }
    }

    @Test
    public void shouldRejectBatchReadBeforeOpen() {
        DataGenSourceReader reader = reader(4L, null, 2);

        try {
            reader.readBatch();
            fail("Expected readBatch before open to be rejected");
        } catch (IllegalStateException failure) {
            assertFalse(failure.getMessage().isEmpty());
        }
    }

    private static DataGenSourceReader reader(long rowCount, Integer splitCount, int batchSize) {
        return new DataGenSourceReader(readerConfig(rowCount, splitCount), batchSize);
    }

    private static DataGenSourceConfig readerConfig(long rowCount, Integer splitCount) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("schema", Collections.singletonList(
                column("id", "bigint", "sequence_start", 1000L)));
        values.put("row_count", rowCount);
        if (splitCount != null) {
            values.put("split_count", splitCount);
        }
        values.put("read_interval_millis", 100L);
        return DataGenSourceConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static Map<String, Object> column(String name, String type, Object... extraPairs) {
        Map<String, Object> column = new LinkedHashMap<String, Object>();
        column.put("name", name);
        column.put("type", type);
        for (int i = 0; i < extraPairs.length; i += 2) {
            column.put(String.valueOf(extraPairs[i]), extraPairs[i + 1]);
        }
        return column;
    }
}
