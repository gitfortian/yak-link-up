package com.link.up.connector.clickhouse.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.List;
import java.util.Map;

/** ClickHouse bounded Source options, aligned with SeaTunnel where practical. */
public final class ClickHouseSourceOptions {

    private ClickHouseSourceOptions() {
    }

    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse HTTP nodes, comma separated, for example host1:8123,host2:8123")
                    .withSemanticType("CLICKHOUSE_HOSTS")
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

    public static final Option<String> TABLE_PATH =
            Options.key("table_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ClickHouse table path in database.table form; may be combined with sql for dataset identity")
                    .withSemanticType("TABLE_PATH")
                    .withScope(ConnectorOptionScope.TASK);

    @SuppressWarnings("rawtypes")
    public static final Option<List<Map>> TABLE_LIST =
            Options.key("table_list")
                    .listType(Map.class)
                    .noDefaultValue()
                    .withDescription("Multi-table bounded read configuration; each item contains table_path or sql")
                    .withSemanticType("TABLE_LIST")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> SQL =
            Options.key("sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Bounded ClickHouse query; sql mode uses a single Link-Up split in this stage")
                    .withSemanticType("SQL")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> FILTER_QUERY =
            Options.key("filter_query")
                    .stringType()
                    .defaultValue("")
                    .withFallbackKeys("scan_filter")
                    .withDescription("ClickHouse-side filter expression without WHERE")
                    .withSemanticType("SQL_FILTER")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<List<String>> PARTITION_LIST =
            Options.key("partition_list")
                    .listType()
                    .noDefaultValue()
                    .withDescription("ClickHouse partition values used to filter system.parts during table-mode planning")
                    .withSemanticType("PARTITION_LIST")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> SPLIT_SIZE =
            Options.key("split.size")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withFallbackKeys("split_size", "request_part_size")
                    .withDescription("Maximum active ClickHouse parts grouped into one Link-Up split")
                    .withSemanticType("SPLIT_SIZE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1024)
                    .withFallbackKeys("scan_batch_rows")
                    .withDescription("Maximum rows emitted from one ClickHouse reader batch")
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
                    .withDescription("Additional ClickHouse Java/JDBC client properties")
                    .withSemanticType("CLICKHOUSE_CLIENT_CONFIG")
                    .withScope(ConnectorOptionScope.DATASOURCE);
}
