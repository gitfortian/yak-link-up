package com.link.up.connector.doris.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.List;
import java.util.Map;

/** Doris bounded native Source options. */
public final class DorisSourceOptions {

    private DorisSourceOptions() {
    }

    // Reuse the existing Doris datasource vocabulary shared with Sink/Catalog.
    public static final Option<String> FENODES = DorisSinkOptions.FENODES;
    public static final Option<Integer> QUERY_PORT = DorisSinkOptions.QUERY_PORT;
    public static final Option<String> USERNAME = DorisSinkOptions.USERNAME;
    public static final Option<String> PASSWORD = DorisSinkOptions.PASSWORD;
    public static final Option<String> DATABASE = DorisSinkOptions.DATABASE;
    public static final Option<String> TABLE = DorisSinkOptions.TABLE;

    @SuppressWarnings("rawtypes")
    public static final Option<List<Map>> TABLE_LIST =
            Options.key("table_list")
                    .listType(Map.class)
                    .noDefaultValue()
                    .withDescription("Doris 多表 bounded read 配置；每项至少包含 table")
                    .withSemanticType("TABLE_LIST")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> READ_FIELDS =
            Options.key("doris.read.field")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("read_fields")
                    .withDescription("读取字段列表，逗号分隔；未配置时读取发现到的全部字段")
                    .withSemanticType("PROJECTION_FIELDS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> FILTER_QUERY =
            Options.key("doris.filter.query")
                    .stringType()
                    .defaultValue("")
                    .withFallbackKeys("scan_filter")
                    .withDescription("下推到 Doris FE Query Plan 的过滤表达式，不包含 WHERE")
                    .withSemanticType("SQL_FILTER")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> REQUEST_TABLET_SIZE =
            Options.key("doris.request.tablet.size")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withFallbackKeys("request_tablet_size")
                    .withDescription("单个 Source Split 最多包含的 Doris Tablet 数量")
                    .withSemanticType("SPLIT_SIZE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> REQUEST_CONNECT_TIMEOUT_MS =
            Options.key("doris.request.connect.timeout.ms")
                    .intType()
                    .defaultValue(30_000)
                    .withFallbackKeys("scan_connect_timeout_ms")
                    .withDescription("连接 Doris FE/BE 的超时时间，单位毫秒")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> REQUEST_READ_TIMEOUT_MS =
            Options.key("doris.request.read.timeout.ms")
                    .intType()
                    .defaultValue(30_000)
                    .withFallbackKeys("scan_read_timeout_ms")
                    .withDescription("读取 Doris FE/BE 响应的超时时间，单位毫秒")
                    .withSemanticType("TIMEOUT_MILLIS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> REQUEST_QUERY_TIMEOUT_SEC =
            Options.key("doris.request.query.timeout.s")
                    .intType()
                    .defaultValue(3600)
                    .withFallbackKeys("scan_query_timeout_sec")
                    .withDescription("Doris BE Scanner 查询超时，单位秒；-1 表示不限制")
                    .withSemanticType("TIMEOUT_SECONDS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> REQUEST_RETRIES =
            Options.key("doris.request.retries")
                    .intType()
                    .defaultValue(3)
                    .withFallbackKeys("max_retries")
                    .withDescription("FE Query Plan / BE Scanner 请求最大重试次数")
                    .withSemanticType("RETRY_COUNT")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Integer> BATCH_SIZE =
            Options.key("doris.batch.size")
                    .intType()
                    .defaultValue(1024)
                    .withFallbackKeys("scan_batch_rows")
                    .withDescription("Doris BE Scanner 每批最大行数")
                    .withSemanticType("BATCH_ROWS")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Long> EXEC_MEM_LIMIT =
            Options.key("doris.exec.mem.limit")
                    .longType()
                    .defaultValue(2_147_483_648L)
                    .withFallbackKeys("scan_mem_limit")
                    .withDescription("单个 Doris BE Scanner 查询内存上限，单位字节")
                    .withSemanticType("MEMORY_BYTES")
                    .withScope(ConnectorOptionScope.RUNTIME);
}
