package com.link.up.connector.print.sink;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.print.config.PrintSinkConfig;
import com.link.up.connector.print.config.PrintSinkOptions;

import java.util.Collections;
import java.util.Set;

/**
 * SPI factory for the Print Sink.
 *
 * <p>The default no-op {@code createPreparer} is kept deliberately: printing
 * has no target-side DDL and no connection to validate, so any preparation
 * action would be an invented side effect.
 */
@AutoService(SinkFactory.class)
public final class PrintSinkFactory implements SinkFactory {

    @Override
    public String factoryIdentifier() {
        return "print";
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.emptySet();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        PrintSinkOptions.PRINT_ROWS,
                        PrintSinkOptions.PRINT_INTERVAL_MILLIS)
                .build();
    }

    @Override
    public SinkWriter<FluxRow> createSink(
            ReadonlyConfig config,
            PreparedSinkMetadata metadata) {

        return new PrintSinkWriter(PrintSinkConfig.of(config));
    }
}
