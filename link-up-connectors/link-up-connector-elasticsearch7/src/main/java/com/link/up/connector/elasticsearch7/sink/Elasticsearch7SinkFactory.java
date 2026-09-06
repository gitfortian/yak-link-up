package com.link.up.connector.elasticsearch7.sink;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.elasticsearch7.Elasticsearch7ConnectorIdentity;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SinkConfig;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SinkOptions;

import java.util.Collections;
import java.util.Set;

/** SPI factory for the bounded Elasticsearch 7 Bulk Sink. */
@AutoService(SinkFactory.class)
public final class Elasticsearch7SinkFactory implements SinkFactory {

    @Override
    public String factoryIdentifier() {
        return Elasticsearch7ConnectorIdentity.IDENTIFIER;
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        // IndexRequest may replace a document when an explicit _id already exists,
        // but Stage 2 does not expose CDC/UPSERT semantics as a connector capability.
        return Collections.emptySet();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        Elasticsearch7SinkOptions.HOSTS,
                        Elasticsearch7SinkOptions.INDEX)
                .optional(
                        Elasticsearch7SinkOptions.USERNAME,
                        Elasticsearch7SinkOptions.PASSWORD,
                        Elasticsearch7SinkOptions.DOCUMENT_ID_FIELD,
                        Elasticsearch7SinkOptions.BATCH_SIZE,
                        Elasticsearch7SinkOptions.MAX_RETRIES,
                        Elasticsearch7SinkOptions.RETRY_BACKOFF_MS,
                        Elasticsearch7SinkOptions.MAX_RETRY_BACKOFF_MS,
                        Elasticsearch7SinkOptions.CONNECT_TIMEOUT_MS,
                        Elasticsearch7SinkOptions.SOCKET_TIMEOUT_MS)
                .build();
    }

    @Override
    public SinkPreparer createPreparer(ReadonlyConfig config) {
        return new Elasticsearch7SinkPreparer(Elasticsearch7SinkConfig.of(config));
    }

    @Override
    public SinkWriter<FluxRow> createSink(
            ReadonlyConfig config,
            PreparedSinkMetadata metadata) {
        return new Elasticsearch7SinkWriter(Elasticsearch7SinkConfig.of(config), metadata);
    }
}
