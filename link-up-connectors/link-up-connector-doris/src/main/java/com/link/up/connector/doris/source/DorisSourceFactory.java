package com.link.up.connector.doris.source;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.doris.catalog.DorisCatalog;
import com.link.up.connector.doris.catalog.DorisCatalogConfig;
import com.link.up.connector.doris.config.DorisSourceConfig;
import com.link.up.connector.doris.config.DorisSourceOptions;
import com.link.up.connector.doris.config.DorisSourceTableConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** SPI factory for the Doris bounded native Source connector. */
@AutoService(TableSourceFactory.class)
public final class DorisSourceFactory implements TableSourceFactory<DorisSourceSplit> {

    private static final String IDENTIFIER = "doris";

    @Override
    public String factoryIdentifier() { return IDENTIFIER; }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY,
                        ConnectorCapability.MULTI_TABLE,
                        ConnectorCapability.PARTITION_SPLIT));
    }

    @Override
    public Source<DorisSourceSplit> createSource(SourceFactoryContext context) {
        return new DorisSource(createConfig(context));
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(SourceFactoryContext context) throws Exception {
        DorisSourceConfig config = createConfig(context);
        DorisCatalogConfig catalogConfig =
                new DorisCatalogConfig(
                        config.getFenodes(),
                        config.getQueryPort(),
                        config.getUsername(),
                        config.getPassword(),
                        null);
        DorisCatalog catalog = new DorisCatalog("doris-native-source", catalogConfig);
        try {
            catalog.open();
            List<CatalogTable> result = new ArrayList<CatalogTable>();
            for (DorisSourceTableConfig tableConfig : config.getTableConfigs()) {
                CatalogTable discovered = catalog.getTable(tableConfig.getTablePath());
                result.add(DorisNativeSourceSchema.prepare(discovered, tableConfig));
            }
            return Collections.unmodifiableList(result);
        } finally {
            catalog.close();
        }
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DorisSourceOptions.FENODES,
                        DorisSourceOptions.USERNAME)
                .optional(
                        DorisSourceOptions.PASSWORD,
                        DorisSourceOptions.QUERY_PORT,
                        DorisSourceOptions.DATABASE,
                        DorisSourceOptions.TABLE,
                        DorisSourceOptions.TABLE_LIST,
                        DorisSourceOptions.READ_FIELDS,
                        DorisSourceOptions.FILTER_QUERY,
                        DorisSourceOptions.REQUEST_TABLET_SIZE,
                        DorisSourceOptions.REQUEST_CONNECT_TIMEOUT_MS,
                        DorisSourceOptions.REQUEST_READ_TIMEOUT_MS,
                        DorisSourceOptions.REQUEST_QUERY_TIMEOUT_SEC,
                        DorisSourceOptions.REQUEST_RETRIES,
                        DorisSourceOptions.BATCH_SIZE,
                        DorisSourceOptions.EXEC_MEM_LIMIT)
                .build();
    }

    private DorisSourceConfig createConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options =
                Objects.requireNonNull(context.getOptions(), "source options must not be null");
        return DorisSourceConfig.of(options);
    }
}
