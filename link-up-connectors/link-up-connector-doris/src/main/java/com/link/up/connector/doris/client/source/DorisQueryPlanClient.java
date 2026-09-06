package com.link.up.connector.doris.client.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.connector.doris.client.source.model.DorisQueryPlan;
import com.link.up.connector.doris.config.DorisSourceConfig;
import com.link.up.connector.doris.config.DorisSourceTableConfig;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Obtains opaque native scan plans from Doris FE over HTTP. */
public final class DorisQueryPlanClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisQueryPlanClient.class);
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private final DorisSourceConfig config;
    private final ObjectMapper objectMapper;
    private final OkHttpClient httpClient;

    public DorisQueryPlanClient(DorisSourceConfig config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
        this.httpClient =
                new OkHttpClient.Builder()
                        .connectTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                        .writeTimeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                        .readTimeout(config.getReadTimeoutMs(), TimeUnit.MILLISECONDS)
                        .build();
    }

    public DorisQueryPlan fetchQueryPlan(
            DorisSourceTableConfig tableConfig,
            CatalogTable table) throws IOException {
        String sql = buildQuerySql(tableConfig, table);
        String requestBody =
                objectMapper.writeValueAsString(java.util.Collections.singletonMap("sql", sql));

        IOException lastFailure = null;
        int attempts = Math.max(1, config.getRequestRetries() + 1);
        List<String> nodes = config.getFeNodes();
        for (int attempt = 0; attempt < attempts; attempt++) {
            String node = nodes.get(attempt % nodes.size());
            String url =
                    normalizeHttpNode(node)
                            + "/api/"
                            + tableConfig.getDatabase()
                            + "/"
                            + tableConfig.getTable()
                            + "/_query_plan";
            Request request =
                    new Request.Builder()
                            .url(url)
                            .header("Authorization", basicAuth())
                            .header("Accept", "application/json")
                            .post(RequestBody.create(requestBody.getBytes(StandardCharsets.UTF_8), JSON))
                            .build();
            try (Response response = httpClient.newCall(request).execute()) {
                ResponseBody body = response.body();
                String text = body == null ? "" : body.string();
                if (!response.isSuccessful()) {
                    throw new IOException(
                            "Doris FE query-plan request failed: httpStatus="
                                    + response.code()
                                    + ", node="
                                    + node
                                    + ", body="
                                    + abbreviate(text, 1000));
                }
                DorisQueryPlan plan = parsePlanResponse(text, objectMapper);
                validatePlan(plan, tableConfig, node);
                return plan;
            } catch (IOException failure) {
                lastFailure = failure;
                LOG.warn(
                        "Doris FE query-plan request failed: table={}.{}, node={}, attempt={}/{}, error={}",
                        tableConfig.getDatabase(),
                        tableConfig.getTable(),
                        node,
                        attempt + 1,
                        attempts,
                        failure.getMessage());
            }
        }
        throw new IOException(
                "Unable to obtain Doris query plan for "
                        + tableConfig.getDatabase()
                        + "."
                        + tableConfig.getTable()
                        + " after "
                        + attempts
                        + " attempts",
                lastFailure);
    }

    static String buildQuerySql(DorisSourceTableConfig tableConfig, CatalogTable table) {
        if (table == null || table.getTableSchema() == null) {
            throw new IllegalArgumentException("Doris native Source requires discovered table schema");
        }
        List<String> fields = new ArrayList<String>();
        for (Column column : table.getTableSchema().getColumns()) {
            fields.add(quoteIdentifier(column.getName()));
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Doris native Source requires at least one projected field");
        }
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
                .append(String.join(", ", fields))
                .append(" FROM ")
                .append(quoteIdentifier(tableConfig.getDatabase()))
                .append('.')
                .append(quoteIdentifier(tableConfig.getTable()));
        if (hasText(tableConfig.getFilterQuery())) {
            sql.append(" WHERE ").append(tableConfig.getFilterQuery().trim());
        }
        return sql.toString();
    }

    static DorisQueryPlan parsePlanResponse(String responseText, ObjectMapper mapper)
            throws IOException {
        if (!hasText(responseText)) {
            throw new IOException("Doris FE returned an empty query-plan response");
        }
        JsonNode root = mapper.readTree(responseText);
        JsonNode planNode = root;
        if (root.has("code") && root.has("msg")) {
            int code = root.path("code").asInt(-1);
            if (code != 0) {
                throw new IOException(
                        "Doris FE query-plan response failed: code="
                                + code
                                + ", msg="
                                + root.path("msg").asText(""));
            }
            planNode = root.get("data");
        }
        if (planNode == null || planNode.isNull()) {
            throw new IOException("Doris FE query-plan response does not contain data");
        }
        return mapper.treeToValue(planNode, DorisQueryPlan.class);
    }

    private static void validatePlan(
            DorisQueryPlan plan,
            DorisSourceTableConfig table,
            String node) throws IOException {
        if (plan == null || plan.getStatus() != 200 || !hasText(plan.getOpaquedQueryPlan())) {
            throw new IOException(
                    "Doris FE returned an invalid query plan: table="
                            + table.getDatabase()
                            + "."
                            + table.getTable()
                            + ", node="
                            + node
                            + ", status="
                            + (plan == null ? "null" : plan.getStatus()));
        }
        if (plan.getPartitions() == null) {
            throw new IOException("Doris FE query plan does not contain partitions");
        }
    }

    private String basicAuth() {
        String credentials = config.getUsername() + ":" + config.getPassword();
        return "Basic "
                + Base64.getEncoder()
                        .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    static String normalizeHttpNode(String node) {
        String value = node.trim();
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            value = "http://" + value;
        }
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static String quoteIdentifier(String value) {
        return "`" + value.replace("`", "``") + "`";
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static String abbreviate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength) + "...";
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }
}
