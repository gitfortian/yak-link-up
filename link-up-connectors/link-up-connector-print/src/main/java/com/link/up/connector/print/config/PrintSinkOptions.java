package com.link.up.connector.print.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

/** User-facing options for the Print Sink. */
public final class PrintSinkOptions {

    private PrintSinkOptions() {
    }

    public static final Option<Boolean> PRINT_ROWS =
            Options.key("print_rows")
                    .booleanType()
                    .defaultValue(Boolean.TRUE)
                    .withDescription("Print row data; when false only the schema line is logged")
                    .withSemanticType("PRINT_ROWS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Long> PRINT_INTERVAL_MILLIS =
            Options.key("print_interval_millis")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Simulated delay after each batch to observe backpressure")
                    .withSemanticType("BATCH_INTERVAL")
                    .withScope(ConnectorOptionScope.RUNTIME);
}
