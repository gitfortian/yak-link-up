package com.link.up.connector.datagen.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataGenSourceConfigTest {

    @Test
    public void parsesMinimalConfigWithDefaults() {
        DataGenSourceConfig config = DataGenSourceConfig.of(
                ReadonlyConfig.fromMap(base()));

        assertEquals(2, config.getColumns().size());
        assertEquals(5L, config.getRowCount());
        assertEquals("datagen_table", config.getTableName());
        assertEquals(0L, config.getReadIntervalMillis());
        assertEquals(false, config.hasPresetRows());
    }

    @Test
    public void shouldRejectRowsWhenColumnCountMismatch() {
        Map<String, Object> values = base();
        values.put("rows", Arrays.asList(
                Arrays.asList("1", "true"),
                Arrays.asList("only-one-cell")));

        try {
            DataGenSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected rows with mismatched column count to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("same width")
                    || failure.getMessage().contains("columns"));
        }
    }

    @Test
    public void shouldRejectSplitCountBeyondTotalRows() {
        Map<String, Object> values = base();
        values.put("row_count", 2L);
        values.put("split_count", 5);

        try {
            DataGenSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected split_count beyond the total row count to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("split_count"));
        }
    }

    @Test
    public void shouldRejectUnsupportedGeneratorType() {
        Map<String, Object> values = base();
        values.put("schema", Collections.singletonList(column("id", "money")));

        try {
            DataGenSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected an unsupported column type to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("money"));
        }
    }

    @Test
    public void shouldRejectRowsTogetherWithRowCount() {
        Map<String, Object> values = base();
        values.put("row_count", 3L);
        values.put("rows", Collections.singletonList(Arrays.asList("1", "true")));

        try {
            DataGenSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected rows and row_count together to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("mutually exclusive"));
        }
    }

    @Test
    public void shouldRejectConflictingGeneratorHints() {
        Map<String, Object> values = base();
        values.put("schema", Collections.singletonList(column(
                "id", "int", "min", 0, "max", 100, "values", Arrays.asList("1", "2"))));

        try {
            DataGenSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected conflicting generator hints to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("conflicting generator hints"));
        }
    }

    @Test
    public void shouldRejectSequenceOverflowWithinRowCount() {
        Map<String, Object> values = base();
        values.put("row_count", 10L);
        values.put("schema", Collections.singletonList(column("id", "int", "sequence_start", 2147483645)));

        try {
            DataGenSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected a sequence overflowing int to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("overflows"));
        }
    }

    @Test
    public void shouldAcceptPresetRowsWithDefaultsAndBytes() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("schema", Arrays.asList(
                column("id", "bigint"),
                column("payload", "bytes")));
        values.put("rows", Collections.singletonList(Arrays.asList("1", "bWlJWmo=")));

        DataGenSourceConfig config = DataGenSourceConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals(1L, config.getRowCount());
        assertTrue(config.hasPresetRows());
    }

    private static Map<String, Object> base() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("schema", Arrays.asList(
                column("id", "int"),
                column("flag", "boolean")));
        return values;
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
