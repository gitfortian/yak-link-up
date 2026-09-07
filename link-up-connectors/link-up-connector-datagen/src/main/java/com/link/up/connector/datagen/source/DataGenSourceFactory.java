package com.link.up.connector.datagen.source;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.datagen.config.ColumnRule;
import com.link.up.connector.datagen.config.DataGenSourceConfig;
import com.link.up.connector.datagen.config.DataGenSourceOptions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** SPI factory for the bounded DataGen Source. */
@AutoService(TableSourceFactory.class)
public final class DataGenSourceFactory
        implements TableSourceFactory<DataGenSourceSplit> {

    @Override
    public String factoryIdentifier() {
        return "datagen";
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.emptySet();
    }

    @Override
    public Source<DataGenSourceSplit> createSource(SourceFactoryContext context) {
        return new DataGenSource(createConfig(context));
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(SourceFactoryContext context) {
        DataGenSourceConfig config = createConfig(context);

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (ColumnRule rule : config.getColumns()) {
            Column.Builder columnBuilder = Column.builder(rule.getName(), rule.getDataType())
                    .nullable(rule.isNullable());
            if (rule.getSqlType() == SqlType.DECIMAL) {
                columnBuilder.precision(rule.getPrecision());
                columnBuilder.scale(rule.getScale());
            }
            schemaBuilder.column(columnBuilder.build());
        }

        CatalogTable table = CatalogTable.builder(config.getTablePath(), schemaBuilder.build())
                .comment("DataGen bounded source table")
                .build();
        return Collections.singletonList(table);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(DataGenSourceOptions.SCHEMA)
                .optional(
                        DataGenSourceOptions.SPLIT_COUNT,
                        DataGenSourceOptions.READ_INTERVAL_MILLIS,
                        DataGenSourceOptions.SEED,
                        DataGenSourceOptions.TABLE_NAME)
                .exclusive(DataGenSourceOptions.ROWS, DataGenSourceOptions.ROW_COUNT)
                .build();
    }

    private static DataGenSourceConfig createConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options = Objects.requireNonNull(
                context.getOptions(),
                "source options must not be null");
        return DataGenSourceConfig.of(options);
    }
}
