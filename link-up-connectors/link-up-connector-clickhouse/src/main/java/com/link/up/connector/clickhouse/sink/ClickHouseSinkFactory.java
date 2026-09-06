package com.link.up.connector.clickhouse.sink;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.clickhouse.config.ClickHouseSinkConfig;
import com.link.up.connector.clickhouse.config.ClickHouseSinkOptions;

import java.util.Collections;
import java.util.Set;

/** SPI factory for the bounded ClickHouse JDBC batch Sink. */
@AutoService(SinkFactory.class)
public final class ClickHouseSinkFactory implements SinkFactory {

    @Override
    public String factoryIdentifier() {
        return "clickhouse";
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.emptySet();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        ClickHouseSinkOptions.HOST,
                        ClickHouseSinkOptions.USERNAME,
                        ClickHouseSinkOptions.DATABASE,
                        ClickHouseSinkOptions.TABLE)
                .optional(
                        ClickHouseSinkOptions.PASSWORD,
                        ClickHouseSinkOptions.BATCH_SIZE,
                        ClickHouseSinkOptions.SERVER_TIME_ZONE,
                        ClickHouseSinkOptions.CLICKHOUSE_CONFIG)
                .build();
    }

    @Override
    public SinkPreparer createPreparer(ReadonlyConfig config) {
        return new ClickHouseSinkPreparer(ClickHouseSinkConfig.of(config));
    }

    @Override
    public SinkWriter<FluxRow> createSink(
            ReadonlyConfig config,
            PreparedSinkMetadata metadata) {
        return new ClickHouseSinkWriter(ClickHouseSinkConfig.of(config), metadata);
    }
}
