package com.link.up.connector.datagen.source;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.datagen.config.DataGenSourceConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataGenRowGeneratorTest {

    @Test
    public void shouldProduceIdenticalDataWithSameSeed() {
        DataGenRowGenerator first = generator(42L);
        DataGenRowGenerator second = generator(42L);

        for (long row = 0; row < 50; row++) {
            assertArrayEquals(
                    "Row " + row + " differs between two generators with the same seed",
                    first.generate(row).toArray(),
                    second.generate(row).toArray());
        }
    }

    @Test
    public void shouldProduceIndependentDataWithDifferentSeed() {
        DataGenRowGenerator first = generator(1L);
        DataGenRowGenerator second = generator(2L);

        boolean differs = false;
        for (long row = 0; row < 10 && !differs; row++) {
            differs = !Arrays.equals(first.generate(row).toArray(), second.generate(row).toArray());
        }
        assertTrue("Different seeds should produce different data", differs);
    }

    @Test
    public void shouldDistributeSequenceContinuouslyAcrossSplits() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("schema", Collections.singletonList(column("id", "bigint", "sequence_start", 1000L)));
        values.put("row_count", 10L);
        values.put("seed", 7L);
        DataGenRowGenerator generator = new DataGenRowGenerator(
                DataGenSourceConfig.of(ReadonlyConfig.fromMap(values)));

        for (long row = 0; row < 10; row++) {
            assertEquals(1000L + row, generator.generate(row).getField(0));
        }
    }

    @Test
    public void shouldPickValuesInclusiveOfBoundaries() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("schema", Collections.singletonList(column(
                "city", "string", "values", Arrays.asList("hangzhou", "shanghai"))));
        values.put("row_count", 200L);
        values.put("seed", 11L);
        DataGenRowGenerator generator = new DataGenRowGenerator(
                DataGenSourceConfig.of(ReadonlyConfig.fromMap(values)));

        boolean firstSeen = false;
        boolean secondSeen = false;
        for (long row = 0; row < 200; row++) {
            String city = (String) generator.generate(row).getField(0);
            if ("hangzhou".equals(city)) {
                firstSeen = true;
            } else if ("shanghai".equals(city)) {
                secondSeen = true;
            } else {
                throw new AssertionError("Unexpected pick value: " + city);
            }
        }
        assertTrue(firstSeen);
        assertTrue(secondSeen);
    }

    @Test
    public void shouldConvertPresetRowsByDeclaredType() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("schema", Arrays.asList(
                column("id", "int"),
                column("flag", "boolean"),
                column("name", "string")));
        values.put("rows", Arrays.asList(
                Arrays.asList("1", "true", "jared"),
                Arrays.asList("2", "false", "mia")));
        DataGenRowGenerator generator = new DataGenRowGenerator(
                DataGenSourceConfig.of(ReadonlyConfig.fromMap(values)));

        FluxRow firstRow = generator.generate(0);
        assertEquals(1, firstRow.getField(0));
        assertEquals(Boolean.TRUE, firstRow.getField(1));
        assertEquals("jared", firstRow.getField(2));

        FluxRow secondRow = generator.generate(1);
        assertEquals(2, secondRow.getField(0));
        assertEquals(Boolean.FALSE, secondRow.getField(1));
        assertEquals("mia", secondRow.getField(2));
    }

    @Test
    public void shouldGenerateIntegralValuesWithinDeclaredRange() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("schema", Collections.singletonList(column("age", "int", "min", 18, "max", 60)));
        values.put("row_count", 500L);
        values.put("seed", 3L);
        DataGenRowGenerator generator = new DataGenRowGenerator(
                DataGenSourceConfig.of(ReadonlyConfig.fromMap(values)));

        for (long row = 0; row < 500; row++) {
            int age = (Integer) generator.generate(row).getField(0);
            assertTrue("age " + age + " below min", age >= 18);
            assertTrue("age " + age + " above max", age <= 60);
        }
    }

    private static DataGenRowGenerator generator(long seed) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("schema", Arrays.asList(
                column("id", "bigint", "min", 0L, "max", 1000000L),
                column("name", "string"),
                column("flag", "boolean")));
        values.put("row_count", 100L);
        values.put("seed", seed);
        return new DataGenRowGenerator(
                DataGenSourceConfig.of(ReadonlyConfig.fromMap(values)));
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
