package com.link.up.connector.mongodb.source;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.mongodb.MongoConnectorIdentity;
import com.link.up.connector.mongodb.catalog.MongoCatalog;
import com.link.up.connector.mongodb.config.MongoCatalogConfig;
import com.link.up.connector.mongodb.config.MongoSourceConfig;
import com.link.up.connector.mongodb.config.MongoSourceOptions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** SPI factory for the bounded MongoDB Source. */
@AutoService(TableSourceFactory.class)
public final class MongoSourceFactory implements TableSourceFactory<MongoSourceSplit> {

    @Override
    public String factoryIdentifier() {
        return MongoConnectorIdentity.IDENTIFIER;
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(ConnectorCapability.TABLE_SCHEMA_DISCOVERY));
    }

    @Override
    public Source<MongoSourceSplit> createSource(SourceFactoryContext context) {
        return new MongoSource(createConfig(context));
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(SourceFactoryContext context)
            throws Exception {
        MongoSourceConfig config = createConfig(context);
        MongoCatalogConfig catalogConfig = MongoCatalogConfig.of(config.getUri());
        try (MongoCatalog catalog = new MongoCatalog(catalogConfig)) {
            catalog.open();
            CatalogTable table = catalog.getTable(config.getTablePath());
            table = projectSelectedFields(table, config.getFields());
            return Collections.singletonList(table);
        }
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        MongoSourceOptions.URI,
                        MongoSourceOptions.COLLECTION)
                .optional(
                        MongoSourceOptions.DATABASE,
                        MongoSourceOptions.FIELDS,
                        MongoSourceOptions.FILTER,
                        MongoSourceOptions.FETCH_SIZE)
                .build();
    }

    private static CatalogTable projectSelectedFields(
            CatalogTable table,
            List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return table;
        }

        TableSchema schema = table.getTableSchema();
        for (String field : fields) {
            if (!schema.contains(field)) {
                throw new IllegalArgumentException(
                        "Selected MongoDB field was not discovered in the collection sample: " + field);
            }
        }
        return table.withSchema(schema.project(fields));
    }

    private static MongoSourceConfig createConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options = Objects.requireNonNull(
                context.getOptions(),
                "source options must not be null");
        return MongoSourceConfig.of(options);
    }
}
