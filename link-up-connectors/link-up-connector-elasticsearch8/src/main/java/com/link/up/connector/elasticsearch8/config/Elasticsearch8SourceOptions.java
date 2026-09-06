package com.link.up.connector.elasticsearch8.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.List;

/** Options for the bounded Elasticsearch 8 Source. */
public final class Elasticsearch8SourceOptions {

    private Elasticsearch8SourceOptions() {
    }

    public static final Option<List<String>> HOSTS =
            Options.key("hosts")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Elasticsearch HTTP nodes, for example http://localhost:9200")
                    .withSemanticType("ELASTICSEARCH_HOSTS")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Elasticsearch username")
                    .withSemanticType("USERNAME")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .defaultValue("")
                    .sensitive()
                    .withDescription("Elasticsearch password")
                    .withSemanticType("PASSWORD")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> INDEX =
            Options.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("One concrete Elasticsearch index or alias resolving to one index")
                    .withSemanticType("INDEX")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<List<String>> SOURCE =
            Options.key("source")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Optional _source projection; dotted nested paths are supported")
                    .withSemanticType("SOURCE_FIELDS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> QUERY =
            Options.key("query")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Optional Elasticsearch Query DSL object; blank means match_all")
                    .withSemanticType("QUERY")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> SCROLL_TIME =
            Options.key("scroll_time")
                    .stringType()
                    .defaultValue("1m")
                    .withDescription("Scroll context keep-alive, supporting ms, s, m, or h")
                    .withSemanticType("SCROLL_TIME")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SCROLL_SIZE =
            Options.key("scroll_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Documents requested from Elasticsearch per scroll page")
                    .withSemanticType("BATCH_ROWS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SLICES =
            Options.key("slices")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Optional fixed number of sliced-scroll splits; defaults to reader parallelism")
                    .withSemanticType("SPLIT_COUNT")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Elasticsearch HTTP connect timeout in milliseconds")
                    .withSemanticType("CONNECT_TIMEOUT_MS")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Elasticsearch HTTP socket timeout in milliseconds")
                    .withSemanticType("SOCKET_TIMEOUT_MS")
                    .withScope(ConnectorOptionScope.DATASOURCE);
}
