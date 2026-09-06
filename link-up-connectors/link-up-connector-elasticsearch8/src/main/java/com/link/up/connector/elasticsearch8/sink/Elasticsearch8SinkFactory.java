package com.link.up.connector.elasticsearch8.sink;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.elasticsearch8.Elasticsearch8ConnectorIdentity;
import com.link.up.connector.elasticsearch8.config.Elasticsearch8SinkConfig;
import com.link.up.connector.elasticsearch8.config.Elasticsearch8SinkOptions;

import java.util.Collections;
import java.util.Set;

/** SPI factory for the bounded Elasticsearch 8 Bulk Sink. */
@AutoService(SinkFactory.class)
public final class Elasticsearch8SinkFactory implements SinkFactory {

    @Override
    public String factoryIdentifier() {
        return Elasticsearch8ConnectorIdentity.IDENTIFIER;
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        // Index operations may replace an existing explicit _id, but Stage 4 does not
        // expose that storage behavior as CDC/UPSERT connector semantics.
        return Collections.emptySet();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        Elasticsearch8SinkOptions.HOSTS,
                        Elasticsearch8SinkOptions.INDEX)
                .optional(
                        Elasticsearch8SinkOptions.USERNAME,
                        Elasticsearch8SinkOptions.PASSWORD,
                        Elasticsearch8SinkOptions.DOCUMENT_ID_FIELD,
                        Elasticsearch8SinkOptions.BATCH_SIZE,
                        Elasticsearch8SinkOptions.MAX_RETRIES,
                        Elasticsearch8SinkOptions.RETRY_BACKOFF_MS,
                        Elasticsearch8SinkOptions.MAX_RETRY_BACKOFF_MS,
                        Elasticsearch8SinkOptions.CONNECT_TIMEOUT_MS,
                        Elasticsearch8SinkOptions.SOCKET_TIMEOUT_MS)
                .build();
    }

    @Override
    public SinkPreparer createPreparer(ReadonlyConfig config) {
        return new Elasticsearch8SinkPreparer(Elasticsearch8SinkConfig.of(config));
    }

    @Override
    public SinkWriter<FluxRow> createSink(
            ReadonlyConfig config,
            PreparedSinkMetadata metadata) {
        return new Elasticsearch8SinkWriter(
                Elasticsearch8SinkConfig.of(config), metadata);
    }
}
