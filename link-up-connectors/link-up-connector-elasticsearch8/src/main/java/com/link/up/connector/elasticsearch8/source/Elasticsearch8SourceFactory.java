package com.link.up.connector.elasticsearch8.source;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.elasticsearch8.Elasticsearch8ConnectorIdentity;
import com.link.up.connector.elasticsearch8.client.Elasticsearch8Client;
import com.link.up.connector.elasticsearch8.config.Elasticsearch8SourceConfig;
import com.link.up.connector.elasticsearch8.config.Elasticsearch8SourceOptions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** SPI factory for the bounded Elasticsearch 8 Source. */
@AutoService(TableSourceFactory.class)
public final class Elasticsearch8SourceFactory
        implements TableSourceFactory<Elasticsearch8SourceSplit> {

    @Override
    public String factoryIdentifier() {
        return Elasticsearch8ConnectorIdentity.IDENTIFIER;
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY,
                        ConnectorCapability.PARTITION_SPLIT));
    }

    @Override
    public Source<Elasticsearch8SourceSplit> createSource(SourceFactoryContext context) {
        return new Elasticsearch8Source(createConfig(context));
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(SourceFactoryContext context)
            throws Exception {
        Elasticsearch8SourceConfig config = createConfig(context);
        try (Elasticsearch8Client client = new Elasticsearch8Client(config)) {
            return Collections.singletonList(client.discoverTable());
        }
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        Elasticsearch8SourceOptions.HOSTS,
                        Elasticsearch8SourceOptions.INDEX)
                .optional(
                        Elasticsearch8SourceOptions.USERNAME,
                        Elasticsearch8SourceOptions.PASSWORD,
                        Elasticsearch8SourceOptions.SOURCE,
                        Elasticsearch8SourceOptions.QUERY,
                        Elasticsearch8SourceOptions.SCROLL_TIME,
                        Elasticsearch8SourceOptions.SCROLL_SIZE,
                        Elasticsearch8SourceOptions.SLICES,
                        Elasticsearch8SourceOptions.CONNECT_TIMEOUT_MS,
                        Elasticsearch8SourceOptions.SOCKET_TIMEOUT_MS)
                .build();
    }

    private Elasticsearch8SourceConfig createConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options = Objects.requireNonNull(
                context.getOptions(), "source options must not be null");
        return Elasticsearch8SourceConfig.of(options);
    }
}
