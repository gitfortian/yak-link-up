package com.link.up.connector.datagen.source;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.connector.datagen.config.DataGenSourceConfig;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataGenSourceSplitEnumeratorTest {

    @Test
    public void shouldSplitRowRangeWithRemainderInLastSplit() {
        DataGenSourceSplitEnumerator enumerator = enumerator(10L, 3);

        List<DataGenSourceSplit> splits = enumerator.enumerateSplits();

        assertEquals(3, splits.size());
        assertRange(splits.get(0), 0L, 4L);
        assertRange(splits.get(1), 4L, 3L);
        assertRange(splits.get(2), 7L, 3L);
    }

    @Test
    public void shouldDefaultSplitCountToParallelism() {
        DataGenSourceSplitEnumerator enumerator = enumerator(10L, null);

        List<DataGenSourceSplit> splits = enumerator.enumerateSplits();

        assertEquals(3, splits.size());
        long covered = 0L;
        for (DataGenSourceSplit split : splits) {
            covered += split.getRowCount();
        }
        assertEquals(10L, covered);
    }

    @Test
    public void shouldClampDefaultSplitCountToTotalRows() {
        DataGenSourceSplitEnumerator enumerator = enumerator(2L, null);

        List<DataGenSourceSplit> splits = enumerator.enumerateSplits();

        assertEquals(2, splits.size());
        assertRange(splits.get(0), 0L, 1L);
        assertRange(splits.get(1), 1L, 1L);
    }

    private static DataGenSourceSplitEnumerator enumerator(long rowCount, Integer splitCount) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("schema", Collections.singletonList(
                column("id", "bigint", "sequence_start", 0L)));
        values.put("row_count", rowCount);
        if (splitCount != null) {
            values.put("split_count", splitCount);
        }

        DataGenSourceConfig config = DataGenSourceConfig.of(ReadonlyConfig.fromMap(values));
        return new DataGenSourceSplitEnumerator(config, 3);
    }

    private static void assertRange(
            DataGenSourceSplit split,
            long expectedStartRow,
            long expectedRowCount) {

        assertEquals(expectedStartRow, split.getStartRow());
        assertEquals(expectedRowCount, split.getRowCount());
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
