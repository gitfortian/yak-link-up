package com.link.up.connector.elasticsearch7.client;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/** Shared ES7 HTTP client construction kept inside the version-specific dependency island. */
final class Elasticsearch7RestClientFactory {

    private Elasticsearch7RestClientFactory() {
    }

    static RestHighLevelClient create(
            List<String> hostValues,
            String username,
            String password,
            int connectTimeoutMs,
            int socketTimeoutMs) {
        HttpHost[] hosts = new HttpHost[hostValues.size()];
        for (int index = 0; index < hostValues.size(); index++) {
            hosts[index] = toHttpHost(hostValues.get(index));
        }

        RestClientBuilder builder = RestClient.builder(hosts);
        builder.setRequestConfigCallback(
                request -> request
                        .setConnectTimeout(connectTimeoutMs)
                        .setSocketTimeout(socketTimeoutMs));

        if (username != null && !username.isEmpty()) {
            BasicCredentialsProvider credentials = new BasicCredentialsProvider();
            credentials.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(
                            username,
                            password == null ? "" : password));
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
}
