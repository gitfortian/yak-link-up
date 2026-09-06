package com.link.up.connector.elasticsearch7.client;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.connector.elasticsearch7.Elasticsearch7ConnectorIdentity;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SourceConfig;
import com.link.up.connector.elasticsearch7.schema.Elasticsearch7TypeMapper;
import com.link.up.connector.elasticsearch7.source.Elasticsearch7SourceSplit;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortBuilders;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** ES7 SDK boundary used by schema discovery and bounded scroll readers. */
public final class Elasticsearch7Client implements AutoCloseable {

    private final Elasticsearch7SourceConfig config;
    private final RestHighLevelClient client;

    public Elasticsearch7Client(Elasticsearch7SourceConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.client = buildClient(config);
    }

    public void verifyMajorVersion() throws IOException {
        MainResponse response = client.info(RequestOptions.DEFAULT);
        String version = response.getVersion().getNumber();
        int dot = version == null ? -1 : version.indexOf('.');
        String majorText = dot < 0 ? version : version.substring(0, dot);
        final int major;
        try {
            major = Integer.parseInt(majorText);
        } catch (RuntimeException failure) {
            throw new IllegalStateException("Cannot parse Elasticsearch server version: " + version, failure);
        }
        if (major != Elasticsearch7ConnectorIdentity.MAJOR_VERSION) {
            throw new IllegalStateException(
                    "Connector '" + Elasticsearch7ConnectorIdentity.IDENTIFIER
                            + "' requires Elasticsearch major version 7, but server reported " + version);
        }
    }

    public CatalogTable discoverTable() throws IOException {
        verifyMajorVersion();
        GetMappingsRequest request = new GetMappingsRequest().indices(config.getIndex());
        GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
        Map<String, MappingMetadata> mappings = response.mappings();
        if (mappings == null || mappings.isEmpty()) {
            throw new IllegalArgumentException("No mapping found for Elasticsearch index: " + config.getIndex());
        }

        String resolvedIndex = config.getIndex();
        MappingMetadata mapping = mappings.get(config.getIndex());
        if (mapping == null) {
            if (mappings.size() != 1) {
                throw new IllegalArgumentException(
                        "Stage 1 Elasticsearch 7 Source requires one concrete index; alias '"
                                + config.getIndex() + "' resolved to " + mappings.keySet());
            }
            Map.Entry<String, MappingMetadata> only = mappings.entrySet().iterator().next();
            resolvedIndex = only.getKey();
            mapping = only.getValue();
        }

        TableSchema schema =
                Elasticsearch7TypeMapper.toTableSchema(
                        mapping.sourceAsMap(),
                        config.getSourceFields());
        return CatalogTable.builder(TablePath.of(config.getIndex()), schema)
                .option("resolved_index", resolvedIndex)
                .build();
    }

    public ScrollPage startScroll(Elasticsearch7SourceSplit split) throws IOException {
        Objects.requireNonNull(split, "split must not be null");
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.size(config.getScrollSize());
        source.sort(SortBuilders.fieldSort("_doc"));
        source.query(
                config.getQuery() == null
                        ? QueryBuilders.matchAllQuery()
                        : QueryBuilders.wrapperQuery(config.getQuery()));
        if (!config.getSourceFields().isEmpty()) {
            source.fetchSource(
                    config.getSourceFields().toArray(new String[0]),
                    new String[0]);
        }
        if (split.getSliceMax() > 1) {
            source.slice(new SliceBuilder(split.getSliceId(), split.getSliceMax()));
        }

        SearchRequest request = new SearchRequest(split.getIndex());
        request.source(source);
        request.scroll(TimeValue.timeValueMillis(config.getScrollTimeMillis()));
        return toPage(client.search(request, RequestOptions.DEFAULT));
    }

    public ScrollPage continueScroll(String scrollId) throws IOException {
        String id = requireText(scrollId, "scrollId");
        SearchScrollRequest request = new SearchScrollRequest(id);
        request.scroll(TimeValue.timeValueMillis(config.getScrollTimeMillis()));
        return toPage(client.scroll(request, RequestOptions.DEFAULT));
    }

    public void clearScroll(String scrollId) throws IOException {
        if (scrollId == null || scrollId.trim().isEmpty()) {
            return;
        }
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(scrollId);
        client.clearScroll(request, RequestOptions.DEFAULT);
    }

    private static ScrollPage toPage(SearchResponse response) {
        List<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> source = hit.getSourceAsMap();
            if (source == null) {
                throw new IllegalStateException(
                        "Elasticsearch document '" + hit.getId()
                                + "' has no _source. Stage 1 bounded Source requires _source to be enabled.");
            }
            documents.add(source);
        }
        return new ScrollPage(response.getScrollId(), documents);
    }

    private static RestHighLevelClient buildClient(Elasticsearch7SourceConfig config) {
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
                    new UsernamePasswordCredentials(
                            config.getUsername(),
                            config.getPassword()));
            builder.setHttpClientConfigCallback(
                    http -> http.setDefaultCredentialsProvider(credentials));
        }
        return new RestHighLevelClient(builder);
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
        client.close();
    }

    /** One neutral scroll page; Elasticsearch SDK response types do not leak into the Reader. */
    public static final class ScrollPage {
        private final String scrollId;
        private final List<Map<String, Object>> documents;

        ScrollPage(String scrollId, List<Map<String, Object>> documents) {
            this.scrollId = scrollId;
            this.documents =
                    Collections.unmodifiableList(
                            new ArrayList<Map<String, Object>>(documents));
        }

        public String getScrollId() {
            return scrollId;
        }

        public List<Map<String, Object>> getDocuments() {
            return documents;
        }
    }
}
