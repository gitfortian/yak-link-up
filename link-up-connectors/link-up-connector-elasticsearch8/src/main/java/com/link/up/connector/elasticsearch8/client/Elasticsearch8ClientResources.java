package com.link.up.connector.elasticsearch8.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/** Package-local ES8 transport resources shared by the bounded Source and Sink. */
final class Elasticsearch8ClientResources implements AutoCloseable {

    private final JacksonJsonpMapper jsonpMapper;
    private final RestClientTransport transport;
    private final ElasticsearchClient client;

    private Elasticsearch8ClientResources(
            JacksonJsonpMapper jsonpMapper,
            RestClientTransport transport,
            ElasticsearchClient client) {
        this.jsonpMapper = jsonpMapper;
        this.transport = transport;
        this.client = client;
    }

    static Elasticsearch8ClientResources create(
            List<String> hosts,
            String username,
            String password,
            int connectTimeoutMs,
            int socketTimeoutMs) {
        HttpHost[] httpHosts = new HttpHost[hosts.size()];
        for (int index = 0; index < hosts.size(); index++) {
            httpHosts[index] = toHttpHost(hosts.get(index));
        }

        RestClientBuilder builder = RestClient.builder(httpHosts);
        builder.setRequestConfigCallback(
                request -> request
                        .setConnectTimeout(connectTimeoutMs)
                        .setSocketTimeout(socketTimeoutMs));
        if (username != null && !username.isEmpty()) {
            BasicCredentialsProvider credentials = new BasicCredentialsProvider();
            credentials.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password == null ? "" : password));
            builder.setHttpClientConfigCallback(
                    http -> http.setDefaultCredentialsProvider(credentials));
        }

        RestClient restClient = builder.build();
        JacksonJsonpMapper mapper = new JacksonJsonpMapper(new ObjectMapper());
        RestClientTransport transport = new RestClientTransport(restClient, mapper);
        return new Elasticsearch8ClientResources(
                mapper,
                transport,
                new ElasticsearchClient(transport));
    }

    JacksonJsonpMapper getJsonpMapper() {
        return jsonpMapper;
    }

    ElasticsearchClient getClient() {
        return client;
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

    @Override
    public void close() throws IOException {
        transport.close();
    }
}
