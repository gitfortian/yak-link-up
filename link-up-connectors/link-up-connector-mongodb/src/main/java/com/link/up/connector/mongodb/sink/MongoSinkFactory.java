package com.link.up.connector.mongodb.sink;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.mongodb.MongoConnectorIdentity;
import com.link.up.connector.mongodb.config.MongoSinkConfig;
import com.link.up.connector.mongodb.config.MongoSinkOptions;

import java.util.Collections;
import java.util.Set;

/** SPI factory for the bounded MongoDB insert Sink. */
@AutoService(SinkFactory.class)
public final class MongoSinkFactory implements SinkFactory {

    @Override
    public String factoryIdentifier() {
        return MongoConnectorIdentity.IDENTIFIER;
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.emptySet();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        MongoSinkOptions.URI,
                        MongoSinkOptions.COLLECTION)
                .optional(
                        MongoSinkOptions.DATABASE,
                        MongoSinkOptions.DOCUMENT_ID_FIELD,
                        MongoSinkOptions.BATCH_SIZE)
                .build();
    }

    @Override
    public SinkPreparer createPreparer(ReadonlyConfig config) {
        return new MongoSinkPreparer(MongoSinkConfig.of(config));
    }

    @Override
    public SinkWriter<FluxRow> createSink(
            ReadonlyConfig config,
            PreparedSinkMetadata metadata) {
        return new MongoSinkWriter(
                MongoSinkConfig.of(config),
                metadata);
    }
}
