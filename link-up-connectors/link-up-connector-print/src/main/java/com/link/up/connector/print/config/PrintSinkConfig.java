package com.link.up.connector.print.config;

import com.link.up.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.util.Objects;

/** Immutable Print Sink configuration, fully validated at construction. */
public final class PrintSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean printRows;
    private final long printIntervalMillis;

    private PrintSinkConfig(
            boolean printRows,
            long printIntervalMillis) {

        this.printRows = printRows;
        this.printIntervalMillis = printIntervalMillis;
    }

    public static PrintSinkConfig of(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config must not be null");

        boolean printRows = config.get(PrintSinkOptions.PRINT_ROWS);
        long printIntervalMillis = config.get(PrintSinkOptions.PRINT_INTERVAL_MILLIS);
        if (printIntervalMillis < 0) {
            throw new IllegalArgumentException("print_interval_millis must not be negative");
        }

        return new PrintSinkConfig(printRows, printIntervalMillis);
    }

    public boolean isPrintRows() {
        return printRows;
    }

    public long getPrintIntervalMillis() {
        return printIntervalMillis;
    }

    @Override
    public String toString() {
        return "PrintSinkConfig{"
                + "printRows=" + printRows
                + ", printIntervalMillis=" + printIntervalMillis
                + '}';
    }
}
