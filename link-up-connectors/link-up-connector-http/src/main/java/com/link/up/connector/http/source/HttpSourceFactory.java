package com.link.up.connector.http.source;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.factory.SourceFactory;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.http.config.HttpSourceConfig;
import com.link.up.connector.http.config.HttpSourceOptions;
import com.link.up.connector.http.schema.HttpSchemaParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * HTTP Source 工厂。
 *
 * <p>负责：
 * <ol>
 *   <li>解析并校验 Source 配置</li>
 *   <li>解析用户定义的 Schema</li>
 *   <li>创建 HTTP Source</li>
 *   <li>返回 Schema 作为 discoverTableSchemas 结果</li>
 * </ol>
 */
@AutoService(TableSourceFactory.class)
public final class HttpSourceFactory
        implements TableSourceFactory<HttpSourceSplit> {

    private static final String IDENTIFIER = "http";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.CUSTOM_SQL));
    }

    @Override
    public Source<HttpSourceSplit> createSource(
            SourceFactoryContext context) throws Exception {

        HttpSourceConfig config = createConfig(context);
        return new HttpSource(config);
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(
            SourceFactoryContext context) throws Exception {

        HttpSourceConfig config = createConfig(context);

        Map<String, Object> schemaFields = config.getSchemaFields();
        if (schemaFields == null || schemaFields.isEmpty()) {
            throw new IllegalArgumentException(
                    "HTTP Source requires 'schema.fields' to define the output schema");
        }

        TableSchema schema = HttpSchemaParser.parse(schemaFields);

        CatalogTable table = CatalogTable.builder(
                        TablePath.of("http"),
                        schema)
                .comment("HTTP Source")
                .build();

        List<CatalogTable> result = new ArrayList<>(1);
        result.add(table);
        return Collections.unmodifiableList(result);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HttpSourceOptions.URL)
                .optional(
                        HttpSourceOptions.METHOD,
                        HttpSourceOptions.HEADERS,
                        HttpSourceOptions.PARAMS,
                        HttpSourceOptions.BODY,
                        HttpSourceOptions.FORMAT,
                        HttpSourceOptions.SCHEMA_FIELDS,
                        HttpSourceOptions.CONTENT_FIELD,
                        HttpSourceOptions.JSON_FIELD,
                        HttpSourceOptions.PAGE_FIELD,
                        HttpSourceOptions.TOTAL_PAGE_SIZE,
                        HttpSourceOptions.PAGE_BATCH_SIZE,
                        HttpSourceOptions.START_PAGE_NUMBER,
                        HttpSourceOptions.PAGE_TYPE,
                        HttpSourceOptions.CURSOR_FIELD,
                        HttpSourceOptions.CURSOR_RESPONSE_FIELD,
                        HttpSourceOptions.USE_PLACEHOLDER_REPLACEMENT,
                        HttpSourceOptions.RETRY,
                        HttpSourceOptions.RETRY_BACKOFF_MULTIPLIER_MS,
                        HttpSourceOptions.RETRY_BACKOFF_MAX_MS,
                        HttpSourceOptions.CONNECT_TIMEOUT_MS,
                        HttpSourceOptions.SOCKET_TIMEOUT_MS,
                        HttpSourceOptions.ENABLE_MULTI_LINES,
                        HttpSourceOptions.JSON_FIELD_MISSED_RETURN_NULL)
                .build();
    }

    private HttpSourceConfig createConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options = Objects.requireNonNull(
                context.getOptions(), "source options must not be null");
        return HttpSourceConfig.of(options);
    }
}
