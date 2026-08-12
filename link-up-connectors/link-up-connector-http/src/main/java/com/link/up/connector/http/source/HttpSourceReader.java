package com.link.up.connector.http.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.source.SourceReader;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.http.client.HttpSourceClient;
import com.link.up.connector.http.config.HttpSourceConfig;
import com.link.up.connector.http.config.PageType;
import com.link.up.connector.http.parser.HttpResponseParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * HTTP 离线数据读取器。
 *
 * <p>Reader 负责：
 * <ol>
 *   <li>执行 HTTP 请求</li>
 *   <li>解析响应数据为 FluxRow</li>
 *   <li>处理分页逻辑</li>
 * </ol>
 */
public final class HttpSourceReader
        implements SourceReader<FluxRow, HttpSourceSplit> {

    private static final Logger LOG =
            LoggerFactory.getLogger(HttpSourceReader.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Configuration JSON_PATH_CONFIG =
            Configuration.builder()
                    .jsonProvider(new JacksonJsonNodeJsonProvider())
                    .mappingProvider(new JacksonMappingProvider())
                    .options(Option.SUPPRESS_EXCEPTIONS)
                    .build();

    private final HttpSourceConfig config;
    private final CatalogTable catalogTable;
    private final int batchSize;
    private final HttpSourceClient client;

    private HttpSourceSplit currentSplit;
    private boolean opened;
    private boolean finished;

    // 分页状态
    private int currentPage;
    private String currentCursor;
    private boolean paginationExhausted;

    // 缓冲：上一次请求解析出的行，尚未全部输出
    private List<FluxRow> buffer = Collections.emptyList();
    private int bufferIndex;

    public HttpSourceReader(
            HttpSourceConfig config,
            Map<TablePath, CatalogTable> tables,
            int batchSize) {

        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }

        this.config = Objects.requireNonNull(config, "config must not be null");
        this.catalogTable = tables.values().iterator().next();
        this.batchSize = batchSize;
        this.client = new HttpSourceClient(config);
    }

    @Override
    public void open(List<HttpSourceSplit> splits) throws Exception {
        if (opened) {
            throw new IllegalStateException("HttpSourceReader has already been opened");
        }
        this.currentSplit = splits.isEmpty() ? new HttpSourceSplit() : splits.get(0);
        this.currentPage = config.getStartPageNumber();
        this.currentCursor = null;
        this.paginationExhausted = !config.hasPagination();
        this.buffer = Collections.emptyList();
        this.bufferIndex = 0;
        this.finished = false;
        this.opened = true;

        LOG.info("HTTP Source Reader opened, url={}, method={}, format={}, pagination={}",
                config.getUrl(), config.getMethod(), config.getFormat(),
                config.hasPagination() ? config.getPageType() : "none");
    }

    @Override
    public RecordBatch<FluxRow> readBatch() throws Exception {
        if (!opened) {
            throw new IllegalStateException("HttpSourceReader has not been opened");
        }
        if (finished) {
            return RecordBatch.endOfInput();
        }

        while (true) {
            // 先从缓冲区取
            while (bufferIndex < buffer.size()) {
                int end = Math.min(bufferIndex + batchSize, buffer.size());
                List<FluxRow> batch = new ArrayList<>(buffer.subList(bufferIndex, end));
                bufferIndex = end;
                return RecordBatch.of(currentSplit, batch);
            }

            // 缓冲区耗尽，获取下一页
            if (paginationExhausted) {
                finished = true;
                return RecordBatch.endOfInput();
            }

            fetchNextPage();
        }
    }

    private void fetchNextPage() throws Exception {
        // 构建当前页的请求参数
        Map<String, String> effectiveHeaders = new LinkedHashMap<>(config.getHeaders());
        Map<String, String> effectiveParams = new LinkedHashMap<>();
        String effectiveBody = config.getBody();

        if (config.hasPagination()) {
            applyPagination(effectiveHeaders, effectiveParams);
        }

        LOG.debug("HTTP request page={}, cursor={}", currentPage, currentCursor);

        String responseBody = client.execute(effectiveHeaders, effectiveParams, effectiveBody);

        // 解析响应
        List<FluxRow> rows = HttpResponseParser.parseResponse(
                responseBody, config, catalogTable.getTableSchema());

        buffer = rows;
        bufferIndex = 0;

        // 判断分页是否结束
        if (config.hasPagination()) {
            advancePagination(responseBody, rows.size());
        } else {
            paginationExhausted = true;
        }
    }

    private void applyPagination(
            Map<String, String> headers,
            Map<String, String> params) {

        if (config.getPageType() == PageType.CURSOR) {
            applyCursorPagination(headers, params);
        } else {
            applyPageNumberPagination(headers, params);
        }
    }

    private void applyPageNumberPagination(
            Map<String, String> headers,
            Map<String, String> params) {

        String pageField = config.getPageField();
        String pageValue = String.valueOf(currentPage);

        if (config.isUsePlaceholderReplacement()) {
            // 占位符替换模式：替换 headers、body 中的 ${page}
            replacePlaceholder(headers, pageField, pageValue);
            replacePlaceholderInMap(params, pageField, pageValue);
        } else {
            // key-based 模式：直接设置参数值
            params.put(pageField, pageValue);
        }
    }

    private void applyCursorPagination(
            Map<String, String> headers,
            Map<String, String> params) {

        if (currentCursor != null && config.getCursorField() != null) {
            if (config.isUsePlaceholderReplacement()) {
                replacePlaceholder(headers, config.getCursorField(), currentCursor);
                replacePlaceholderInMap(params, config.getCursorField(), currentCursor);
            } else {
                params.put(config.getCursorField(), currentCursor);
            }
        }
    }

    private void advancePagination(String responseBody, int rowCount) {
        if (config.getPageType() == PageType.CURSOR) {
            advanceCursorPagination(responseBody);
        } else {
            advancePageNumberPagination(rowCount);
        }
    }

    private void advancePageNumberPagination(int rowCount) {
        long totalPageSize = config.getTotalPageSize();

        if (totalPageSize > 0) {
            // 已知总页数
            if (currentPage >= totalPageSize + config.getStartPageNumber() - 1) {
                paginationExhausted = true;
                return;
            }
        } else {
            // 未知总页数，根据返回行数判断
            if (rowCount < config.getPageBatchSize()) {
                paginationExhausted = true;
                return;
            }
        }

        currentPage++;
    }

    private void advanceCursorPagination(String responseBody) {
        if (config.getCursorResponseField() == null || config.getCursorResponseField().isEmpty()) {
            paginationExhausted = true;
            return;
        }

        try {
            JsonNode root = MAPPER.readTree(responseBody);
            String normalizedPath = config.getCursorResponseField()
                    .replaceAll("\\.\\*", "[*]");
            Object result = JsonPath.using(JSON_PATH_CONFIG).parse(root).read(normalizedPath);

            if (result == null) {
                paginationExhausted = true;
                return;
            }

            String cursorValue;
            if (result instanceof JsonNode) {
                JsonNode node = (JsonNode) result;
                if (node.isNull() || node.isMissingNode()) {
                    paginationExhausted = true;
                    return;
                }
                cursorValue = node.isTextual() ? node.asText() : node.toString();
            } else {
                cursorValue = String.valueOf(result);
            }

            if (cursorValue.isEmpty() || "null".equals(cursorValue)) {
                paginationExhausted = true;
                return;
            }

            currentCursor = cursorValue;
        } catch (Exception e) {
            LOG.warn("Failed to extract cursor from response, pagination ended: {}", e.getMessage());
            paginationExhausted = true;
        }
    }

    // ── 占位符替换 ──────────────────────────────────────────

    private static void replacePlaceholder(Map<String, String> map, String field, String value) {
        if (map == null) return;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String v = entry.getValue();
            if (v != null && v.contains("${" + field + "}")) {
                entry.setValue(v.replace("${" + field + "}", value));
            }
        }
    }

    private static void replacePlaceholderInMap(Map<String, String> map, String field, String value) {
        replacePlaceholder(map, field, value);
    }

    @Override
    public void close() throws Exception {
        if (!opened) {
            return;
        }
        client.close();
        opened = false;
        LOG.info("HTTP Source Reader closed");
    }
}
