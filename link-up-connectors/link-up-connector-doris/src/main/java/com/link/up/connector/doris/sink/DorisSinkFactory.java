package com.link.up.connector.doris.sink;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.factory.SinkFactory;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.doris.config.DorisSinkConfig;
import com.link.up.connector.doris.config.DorisSinkOptions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Doris Sink SPI factory。
 *
 * <p>通过 Stream Load 将数据写入 Apache Doris，
 * 支持 JSON / CSV 格式、两阶段提交、自动建表等能力。
 */
@AutoService(SinkFactory.class)
public final class DorisSinkFactory implements SinkFactory {

    @Override
    public String factoryIdentifier() {
        return "doris";
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.AUTO_CREATE_TABLE,
                        ConnectorCapability.UPSERT));
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DorisSinkOptions.FENODES,
                        DorisSinkOptions.USERNAME,
                        DorisSinkOptions.DATABASE,
                        DorisSinkOptions.TABLE)
                .optional(
                        DorisSinkOptions.BENODES,
                        DorisSinkOptions.DIRECT_TO_BE,
                        DorisSinkOptions.QUERY_PORT,
                        DorisSinkOptions.PASSWORD,
                        DorisSinkOptions.SINK_LABEL_PREFIX,
                        DorisSinkOptions.SINK_ENABLE_2PC,
                        DorisSinkOptions.SINK_ENABLE_DELETE,
                        DorisSinkOptions.SINK_CHECK_INTERVAL_MS,
                        DorisSinkOptions.SINK_MAX_RETRIES,
                        DorisSinkOptions.SINK_BUFFER_SIZE,
                        DorisSinkOptions.SINK_BUFFER_COUNT,
                        DorisSinkOptions.DORIS_BATCH_SIZE,
                        DorisSinkOptions.LOAD_FORMAT,
                        DorisSinkOptions.CSV_COLUMN_SEPARATOR,
                        DorisSinkOptions.DORIS_CONFIG,
                        DorisSinkOptions.CONNECT_TIMEOUT_MS,
                        DorisSinkOptions.SOCKET_TIMEOUT_MS)
                .build();
    }

    @Override
    public SinkWriter<FluxRow> createSink(
            ReadonlyConfig config,
            PreparedSinkMetadata metadata) {

        return new DorisSinkWriter(
                DorisSinkConfig.of(config),
                metadata);
    }
}
