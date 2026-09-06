package com.link.up.connector.elasticsearch7.source;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.elasticsearch7.Elasticsearch7ConnectorIdentity;
import com.link.up.connector.elasticsearch7.client.Elasticsearch7Client;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SourceConfig;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SourceOptions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** SPI factory for the bounded Elasticsearch 7 Source. */
@AutoService(TableSourceFactory.class)
public final class Elasticsearch7SourceFactory
        implements TableSourceFactory<Elasticsearch7SourceSplit> {

    @Override
    public String factoryIdentifier() {
        return Elasticsearch7ConnectorIdentity.IDENTIFIER;
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY,
                        ConnectorCapability.PARTITION_SPLIT));
    }

    @Override
    public Source<Elasticsearch7SourceSplit> createSource(SourceFactoryContext context) {
        return new Elasticsearch7Source(createConfig(context));
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(SourceFactoryContext context)
            throws Exception {
        Elasticsearch7SourceConfig config = createConfig(context);
        try (Elasticsearch7Client client = new Elasticsearch7Client(config)) {
            return Collections.singletonList(client.discoverTable());
        }
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        Elasticsearch7SourceOptions.HOSTS,
                        Elasticsearch7SourceOptions.INDEX)
                .optional(
                        Elasticsearch7SourceOptions.USERNAME,
                        Elasticsearch7SourceOptions.PASSWORD,
                        Elasticsearch7SourceOptions.SOURCE,
                        Elasticsearch7SourceOptions.QUERY,
                        Elasticsearch7SourceOptions.SCROLL_TIME,
                        Elasticsearch7SourceOptions.SCROLL_SIZE,
                        Elasticsearch7SourceOptions.SLICES,
                        Elasticsearch7SourceOptions.CONNECT_TIMEOUT_MS,
                        Elasticsearch7SourceOptions.SOCKET_TIMEOUT_MS)
                .build();
    }

    private Elasticsearch7SourceConfig createConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options =
                Objects.requireNonNull(context.getOptions(), "source options must not be null");
        return Elasticsearch7SourceConfig.of(options);
    }
}
