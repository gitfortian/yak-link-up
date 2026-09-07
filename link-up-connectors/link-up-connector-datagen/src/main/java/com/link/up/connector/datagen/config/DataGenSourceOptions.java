package com.link.up.connector.datagen.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.List;
import java.util.Map;

/** User-facing options for the bounded DataGen Source. */
public final class DataGenSourceOptions {

    private DataGenSourceOptions() {
    }

    public static final Option<List<Map>> SCHEMA =
            Options.key("schema")
                    .listType(Map.class)
                    .noDefaultValue()
                    .withDescription(
                            "Column definitions as {name, type, nullable, min, max, length, values, sequence_start}; "
                                    + "generator hints are optional per column")
                    .withSemanticType("SOURCE_SCHEMA")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Long> ROW_COUNT =
            Options.key("row_count")
                    .longType()
                    .defaultValue(5L)
                    .withDescription("Total row count of the whole job, not per reader")
                    .withSemanticType("SOURCE_ROWS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<List<List>> ROWS =
            Options.key("rows")
                    .listType(List.class)
                    .noDefaultValue()
                    .withDescription(
                            "Explicit preset rows in schema column order; overrides row_count; "
                                    + "bytes values are base64 encoded")
                    .withSemanticType("SOURCE_ROWS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> SPLIT_COUNT =
            Options.key("split_count")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Bounded split count; defaults to the reader parallelism")
                    .withSemanticType("SPLIT_COUNT")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Long> READ_INTERVAL_MILLIS =
            Options.key("read_interval_millis")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Simulated delay between batches to observe backpressure")
                    .withSemanticType("BATCH_INTERVAL")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Long> SEED =
            Options.key("seed")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Random seed; the same seed reproduces the same data set")
                    .withSemanticType("RANDOM_SEED")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .defaultValue("datagen_table")
                    .withDescription("Logical table name used as the data set id")
                    .withSemanticType("TABLE")
                    .withScope(ConnectorOptionScope.TASK);
}
