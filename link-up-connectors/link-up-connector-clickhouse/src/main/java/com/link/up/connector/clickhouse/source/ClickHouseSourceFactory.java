package com.link.up.connector.clickhouse.source;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.clickhouse.client.ClickHouseJdbcClient;
import com.link.up.connector.clickhouse.config.ClickHouseSourceConfig;
import com.link.up.connector.clickhouse.config.ClickHouseSourceOptions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** SPI factory for the standalone bounded ClickHouse Source. */
@AutoService(TableSourceFactory.class)
public final class ClickHouseSourceFactory
        implements TableSourceFactory<ClickHouseSourceSplit> {

    private static final String IDENTIFIER = "clickhouse";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY,
                        ConnectorCapability.MULTI_TABLE,
                        ConnectorCapability.PARTITION_SPLIT));
    }

    @Override
    public Source<ClickHouseSourceSplit> createSource(SourceFactoryContext context) {
        return new ClickHouseSource(createConfig(context));
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(SourceFactoryContext context)
            throws Exception {
        ClickHouseSourceConfig config = createConfig(context);
        try (ClickHouseJdbcClient client = new ClickHouseJdbcClient(config)) {
            return client.discoverTables();
        }
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        ClickHouseSourceOptions.HOST,
                        ClickHouseSourceOptions.USERNAME)
                .optional(
                        ClickHouseSourceOptions.PASSWORD,
                        ClickHouseSourceOptions.TABLE_PATH,
                        ClickHouseSourceOptions.TABLE_LIST,
                        ClickHouseSourceOptions.SQL,
                        ClickHouseSourceOptions.FILTER_QUERY,
                        ClickHouseSourceOptions.PARTITION_LIST,
                        ClickHouseSourceOptions.SPLIT_SIZE,
                        ClickHouseSourceOptions.BATCH_SIZE,
                        ClickHouseSourceOptions.SERVER_TIME_ZONE,
                        ClickHouseSourceOptions.CLICKHOUSE_CONFIG)
                .build();
    }

    private ClickHouseSourceConfig createConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options =
                Objects.requireNonNull(context.getOptions(), "source options must not be null");
        return ClickHouseSourceConfig.of(options);
    }
}
