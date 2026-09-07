package com.link.up.connector.print.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PrintSinkConfigTest {

    @Test
    public void shouldDefaultPrintRowsToTrue() {
        PrintSinkConfig config = PrintSinkConfig.of(ReadonlyConfig.fromMap(
                new LinkedHashMap<String, Object>()));

        assertTrue(config.isPrintRows());
        assertEquals(0L, config.getPrintIntervalMillis());
    }

    @Test
    public void shouldAcceptExplicitPrintRowsFalse() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("print_rows", false);

        PrintSinkConfig config = PrintSinkConfig.of(ReadonlyConfig.fromMap(values));

        assertFalse(config.isPrintRows());
    }

    @Test
    public void shouldRejectNegativeInterval() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("print_interval_millis", -1L);

        try {
            PrintSinkConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected a negative print_interval_millis to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("print_interval_millis"));
        }
    }
}
