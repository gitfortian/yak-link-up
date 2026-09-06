package com.link.up.connector.clickhouse.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.Map;

/** Options for the bounded ClickHouse JDBC batch Sink. */
public final class ClickHouseSinkOptions {

    private ClickHouseSinkOptions() {
    }

    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Single ClickHouse HTTP endpoint used for bounded batch inserts")
                    .withSemanticType("CLICKHOUSE_HOST")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse username")
                    .withSemanticType("USERNAME")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .defaultValue("")
                    .sensitive()
                    .withDescription("ClickHouse password")
                    .withSemanticType("PASSWORD")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target ClickHouse database")
                    .withSemanticType("DATABASE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Target ClickHouse table")
                    .withSemanticType("TABLE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> BATCH_SIZE =
            Options.key("sink.batch_size")
                    .intType()
                    .defaultValue(10000)
                    .withFallbackKeys("batch_size", "sink.batch-size")
                    .withDescription("Rows accumulated before one synchronous ClickHouse executeBatch call")
                    .withSemanticType("BATCH_ROWS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<String> SERVER_TIME_ZONE =
            Options.key("server_time_zone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Explicit ClickHouse server/session time zone used by the JDBC client")
                    .withSemanticType("TIME_ZONE")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<Map<String, String>> CLICKHOUSE_CONFIG =
            Options.key("clickhouse.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Additional ClickHouse JDBC client properties; unsafe async acknowledgement is rejected")
                    .withSemanticType("CLICKHOUSE_CLIENT_CONFIG")
                    .withScope(ConnectorOptionScope.DATASOURCE);
}
