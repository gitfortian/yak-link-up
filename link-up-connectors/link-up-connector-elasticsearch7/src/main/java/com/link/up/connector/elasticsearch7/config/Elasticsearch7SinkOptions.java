package com.link.up.connector.elasticsearch7.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.List;

/** Options for the bounded Elasticsearch 7 Sink. */
public final class Elasticsearch7SinkOptions {

    private Elasticsearch7SinkOptions() {
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
                    .withDescription("One existing Elasticsearch target index or single-index alias")
                    .withSemanticType("INDEX")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> DOCUMENT_ID_FIELD =
            Options.key("document_id_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional source field used as Elasticsearch _id for deterministic re-indexing")
                    .withSemanticType("DOCUMENT_ID_FIELD")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum documents in one synchronous Elasticsearch Bulk request")
                    .withSemanticType("BATCH_ROWS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Retries for explicit retryable Bulk item failures; transport-level failures are never retried")
                    .withSemanticType("MAX_RETRIES")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Long> RETRY_BACKOFF_MS =
            Options.key("retry_backoff_ms")
                    .longType()
                    .defaultValue(200L)
                    .withDescription("Initial backoff for retryable Bulk item failures")
                    .withSemanticType("RETRY_BACKOFF_MS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Long> MAX_RETRY_BACKOFF_MS =
            Options.key("max_retry_backoff_ms")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription("Maximum backoff for retryable Bulk item failures")
                    .withSemanticType("MAX_RETRY_BACKOFF_MS")
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
