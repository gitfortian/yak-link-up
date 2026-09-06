package com.link.up.connector.elasticsearch8.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.InfoResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.ResponseBody;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.connector.elasticsearch8.Elasticsearch8ConnectorIdentity;
import com.link.up.connector.elasticsearch8.config.Elasticsearch8SourceConfig;
import com.link.up.connector.elasticsearch8.schema.Elasticsearch8TypeMapper;
import com.link.up.connector.elasticsearch8.source.Elasticsearch8SourceSplit;
import jakarta.json.stream.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** ES8 Java API Client boundary for mapping discovery and bounded scroll reads. */
public final class Elasticsearch8Client implements AutoCloseable {

    private final Elasticsearch8SourceConfig config;
    private final JacksonJsonpMapper jsonpMapper;
    private final RestClientTransport transport;
    private final ElasticsearchClient client;

    public Elasticsearch8Client(Elasticsearch8SourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        RestClient restClient = buildRestClient(config);
        this.jsonpMapper = new JacksonJsonpMapper(new ObjectMapper());
        this.transport = new RestClientTransport(restClient, jsonpMapper);
        this.client = new ElasticsearchClient(transport);
    }

    public void verifyMajorVersion() throws IOException {
        InfoResponse response = client.info();
        String version = response.version().number();
        int dot = version == null ? -1 : version.indexOf('.');
        String majorText = dot < 0 ? version : version.substring(0, dot);
        final int major;
        try {
            major = Integer.parseInt(majorText);
        } catch (RuntimeException failure) {
            throw new IllegalStateException("Cannot parse Elasticsearch server version: " + version, failure);
        }
        if (major != Elasticsearch8ConnectorIdentity.MAJOR_VERSION) {
            throw new IllegalStateException(
                    "Connector '" + Elasticsearch8ConnectorIdentity.IDENTIFIER
                            + "' requires Elasticsearch major version 8, but server reported " + version);
        }
    }

    public CatalogTable discoverTable() throws IOException {
        verifyMajorVersion();
        GetMappingResponse response =
                client.indices().getMapping(request -> request.index(config.getIndex()));
        Map<String, IndexMappingRecord> mappings = response.result();
        if (mappings == null || mappings.isEmpty()) {
            throw new IllegalArgumentException("No mapping found for Elasticsearch index: " + config.getIndex());
        }

        String resolvedIndex = config.getIndex();
        IndexMappingRecord record = mappings.get(config.getIndex());
        if (record == null) {
            if (mappings.size() != 1) {
                throw new IllegalArgumentException(
                        "Stage 3 Elasticsearch 8 Source requires one concrete index; alias '"
                                + config.getIndex() + "' resolved to " + mappings.keySet());
            }
            Map.Entry<String, IndexMappingRecord> only = mappings.entrySet().iterator().next();
            resolvedIndex = only.getKey();
            record = only.getValue();
        }

        TableSchema schema =
                Elasticsearch8TypeMapper.toTableSchema(
                        record.mappings(),
                        config.getSourceFields());
        return CatalogTable.builder(TablePath.of(config.getIndex()), schema)
                .option("resolved_index", resolvedIndex)
                .build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public ScrollPage startScroll(Elasticsearch8SourceSplit split) throws IOException {
        Objects.requireNonNull(split, "split must not be null");
        SearchRequest.Builder builder = new SearchRequest.Builder()
                .index(split.getIndex())
                .size(config.getScrollSize())
                .scroll(time -> time.time(config.getScrollTime()))
                .sort(sort -> sort.doc(doc -> doc))
                .query(config.getQuery() == null
                        ? Query.of(query -> query.matchAll(matchAll -> matchAll))
                        : parseQuery(config.getQuery()));

        if (!config.getSourceFields().isEmpty()) {
            builder.source(source -> source.filter(
                    filter -> filter.includes(config.getSourceFields())));
        }
        if (split.getSliceMax() > 1) {
            builder.slice(slice -> slice
                    .id(String.valueOf(split.getSliceId()))
                    .max(split.getSliceMax()));
        }

        SearchResponse<Map> response = client.search(builder.build(), Map.class);
        return toPage(response);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public ScrollPage continueScroll(String scrollId) throws IOException {
        String id = requireText(scrollId, "scrollId");
        ScrollRequest request = ScrollRequest.of(builder -> builder
                .scrollId(id)
                .scroll(time -> time.time(config.getScrollTime())));
        ScrollResponse<Map> response = client.scroll(request, Map.class);
        return toPage(response);
    }

    public void clearScroll(String scrollId) throws IOException {
        if (scrollId == null || scrollId.trim().isEmpty()) {
            return;
        }
        ClearScrollRequest request = ClearScrollRequest.of(builder -> builder.scrollId(scrollId));
        client.clearScroll(request);
    }

    private Query parseQuery(String json) {
        JsonParser parser = jsonpMapper.jsonProvider().createParser(new StringReader(json));
        try {
            return Query._DESERIALIZER.deserialize(parser, jsonpMapper);
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException("Invalid Elasticsearch 8 Query DSL JSON", failure);
        } finally {
            parser.close();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static ScrollPage toPage(ResponseBody<Map> response) {
        List<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
        for (Hit<Map> hit : response.hits().hits()) {
            Map source = hit.source();
            if (source == null) {
                throw new IllegalStateException(
                        "Elasticsearch document '" + hit.id()
                                + "' has no _source. Stage 3 bounded Source requires _source to be enabled.");
            }
            documents.add(new java.util.LinkedHashMap<String, Object>((Map<String, Object>) source));
        }
        return new ScrollPage(response.scrollId(), documents);
    }

    private static RestClient buildRestClient(Elasticsearch8SourceConfig config) {
        HttpHost[] hosts = new HttpHost[config.getHosts().size()];
        for (int index = 0; index < config.getHosts().size(); index++) {
            hosts[index] = toHttpHost(config.getHosts().get(index));
        }

        RestClientBuilder builder = RestClient.builder(hosts);
        builder.setRequestConfigCallback(
                request -> request
                        .setConnectTimeout(config.getConnectTimeoutMs())
                        .setSocketTimeout(config.getSocketTimeoutMs()));
        if (!config.getUsername().isEmpty()) {
            BasicCredentialsProvider credentials = new BasicCredentialsProvider();
            credentials.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            builder.setHttpClientConfigCallback(
                    http -> http.setDefaultCredentialsProvider(credentials));
        }
        return builder.build();
    }

    private static HttpHost toHttpHost(String value) {
        final URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException failure) {
            throw new IllegalArgumentException("Invalid Elasticsearch host: " + value, failure);
        }
        int port = uri.getPort();
        if (port < 0) {
            port = "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 9200;
        }
        return new HttpHost(uri.getHost(), port, uri.getScheme());
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value.trim();
    }

    @Override
    public void close() throws IOException {
        transport.close();
    }

    /** One connector-neutral scroll page; SDK response types do not leak into the Reader. */
    public static final class ScrollPage {
        private final String scrollId;
        private final List<Map<String, Object>> documents;

        ScrollPage(String scrollId, List<Map<String, Object>> documents) {
            this.scrollId = scrollId;
            this.documents = Collections.unmodifiableList(
                    new ArrayList<Map<String, Object>>(documents));
        }

        public String getScrollId() { return scrollId; }
        public List<Map<String, Object>> getDocuments() { return documents; }
    }
}
