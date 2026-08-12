package com.link.up.connector.doris.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.Map;

/**
 * Doris Sink 配置项。
 *
 * <p>参考 SeaTunnel Doris Sink 参数设计，
 * 内部通过 Stream Load 将数据批量写入 Doris。
 */
public final class DorisSinkOptions {

    private DorisSinkOptions() {
    }

    // ── 连接配置 ──────────────────────────────────────────

    public static final Option<String> FENODES =
            Options.key("fenodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Doris 集群 FE HTTP 地址，格式 fe_ip:fe_http_port，多个逗号分隔")
                    .withSemanticType("DORIS_FENODES")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> BENODES =
            Options.key("benodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Doris BE HTTP 地址列表，direct_to_be=true 时使用")
                    .withSemanticType("DORIS_BENODES")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<Boolean> DIRECT_TO_BE =
            Options.key("direct_to_be")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否将 Stream Load 请求直接发送到 BE 节点")
                    .withSemanticType("DORIS_DIRECT_TO_BE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> QUERY_PORT =
            Options.key("query-port")
                    .intType()
                    .defaultValue(9030)
                    .withDescription("Doris FE MySQL 协议查询端口")
                    .withSemanticType("DORIS_QUERY_PORT")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Doris 用户名")
                    .withSemanticType("USERNAME")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .sensitive()
                    .withDescription("Doris 密码")
                    .withSemanticType("PASSWORD")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    // ── 目标表配置 ──────────────────────────────────────────

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Doris 数据库名")
                    .withSemanticType("DATABASE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Doris 表名")
                    .withSemanticType("TABLE")
                    .withScope(ConnectorOptionScope.TASK);

    // ── Stream Load 配置 ──────────────────────────────────────

    public static final Option<String> SINK_LABEL_PREFIX =
            Options.key("sink.label-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stream Load 标签前缀，2PC 场景需全局唯一")
                    .withSemanticType("LABEL_PREFIX")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Boolean> SINK_ENABLE_2PC =
            Options.key("sink.enable-2pc")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否启用两阶段提交")
                    .withSemanticType("ENABLE_2PC")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Boolean> SINK_ENABLE_DELETE =
            Options.key("sink.enable-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否启用删除（需 Doris 表开启批量删除，仅 Unique 模型）")
                    .withSemanticType("ENABLE_DELETE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> SINK_CHECK_INTERVAL_MS =
            Options.key("sink.check-interval")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("检查加载异常的时间间隔，单位毫秒")
                    .withSemanticType("CHECK_INTERVAL")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SINK_MAX_RETRIES =
            Options.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("写入失败最大重试次数")
                    .withSemanticType("MAX_RETRIES")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SINK_BUFFER_SIZE =
            Options.key("sink.buffer-size")
                    .intType()
                    .defaultValue(262144)
                    .withDescription("Stream Load 数据缓冲区大小（字节），默认 256KB")
                    .withSemanticType("BUFFER_SIZE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> SINK_BUFFER_COUNT =
            Options.key("sink.buffer-count")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Stream Load 数据缓冲区计数")
                    .withSemanticType("BUFFER_COUNT")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> DORIS_BATCH_SIZE =
            Options.key("doris.batch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("每次 HTTP 请求写入的行数阈值")
                    .withSemanticType("BATCH_SIZE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    // ── 数据格式 ──────────────────────────────────────────

    public static final Option<DorisLoadFormat> LOAD_FORMAT =
            Options.key("doris.load-format")
                    .enumType(DorisLoadFormat.class)
                    .defaultValue(DorisLoadFormat.JSON)
                    .withDescription("Stream Load 数据格式：JSON 或 CSV")
                    .withSemanticType("LOAD_FORMAT")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> CSV_COLUMN_SEPARATOR =
            Options.key("doris.csv.column-separator")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("CSV 格式的列分隔符")
                    .withSemanticType("CSV_SEPARATOR")
                    .withScope(ConnectorOptionScope.TASK);

    // ── Doris 额外配置 ──────────────────────────────────────

    /**
     * 透传给 Doris Stream Load 的额外配置。
     *
     * <p>例如 format、read_json_by_line、column_separator 等。
     */
    public static final Option<Map<String, String>> DORIS_CONFIG =
            Options.key("doris.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("透传给 Doris Stream Load 的额外 HTTP Header 配置")
                    .withSemanticType("DORIS_CONFIG")
                    .withScope(ConnectorOptionScope.TASK);

    // ── 超时 ──────────────────────────────────────────────

    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("HTTP 连接超时，单位毫秒")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(300000)
                    .withDescription("HTTP Socket 读取超时，单位毫秒（Stream Load 可能较慢）")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.TASK);
}
