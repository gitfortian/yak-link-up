package com.link.up.connector.elasticsearch7.client;

import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.connector.elasticsearch7.Elasticsearch7ConnectorIdentity;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SinkConfig;
import com.link.up.connector.elasticsearch7.converter.Elasticsearch7DocumentConverter.Document;
import com.link.up.connector.elasticsearch7.schema.Elasticsearch7TypeMapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** ES7 SDK boundary for target mapping discovery and synchronous bounded Bulk writes. */
public final class Elasticsearch7SinkClient implements AutoCloseable {

    private final Elasticsearch7SinkConfig config;
    private final RestHighLevelClient client;

    public Elasticsearch7SinkClient(Elasticsearch7SinkConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.client =
                Elasticsearch7RestClientFactory.create(
                        config.getHosts(),
                        config.getUsername(),
                        config.getPassword(),
                        config.getConnectTimeoutMs(),
                        config.getSocketTimeoutMs());
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

    public CatalogTable discoverTargetTable(List<String> projectedFields) throws IOException {
        verifyMajorVersion();
        GetMappingsRequest request = new GetMappingsRequest().indices(config.getIndex());
        GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
        Map<String, MappingMetadata> mappings = response.mappings();
        if (mappings == null || mappings.isEmpty()) {
            throw new IllegalArgumentException(
                    "Elasticsearch 7 Sink target index must already exist and contain a mapping: "
                            + config.getIndex());
        }

        String resolvedIndex = config.getIndex();
        MappingMetadata mapping = mappings.get(config.getIndex());
        if (mapping == null) {
            if (mappings.size() != 1) {
                throw new IllegalArgumentException(
                        "Stage 2 Elasticsearch 7 Sink requires one concrete target index; alias '"
                                + config.getIndex() + "' resolved to " + mappings.keySet());
            }
            Map.Entry<String, MappingMetadata> only = mappings.entrySet().iterator().next();
            resolvedIndex = only.getKey();
            mapping = only.getValue();
        }

        TableSchema schema =
                Elasticsearch7TypeMapper.toTableSchema(mapping.sourceAsMap(), projectedFields);
        return CatalogTable.builder(TablePath.of(config.getIndex()), schema)
                .option("resolved_index", resolvedIndex)
                .build();
    }

    /**
     * Indexes all supplied documents. Only explicit retryable item failures are retried.
     * IOException from the whole Bulk call is deliberately propagated without retry because
     * server-side success is ambiguous at that boundary.
     */
    public int bulkIndex(List<Document> documents) throws IOException {
        Objects.requireNonNull(documents, "documents must not be null");
        if (documents.isEmpty()) {
            return 0;
        }

        List<Document> pending = new ArrayList<Document>(documents);
        int retryNumber = 0;
        while (true) {
            BulkRequest request = new BulkRequest();
            for (Document document : pending) {
                IndexRequest indexRequest =
                        new IndexRequest(config.getIndex()).source(document.getSource());
                if (document.getId() != null) {
                    indexRequest.id(document.getId());
                }
                request.add(indexRequest);
            }

            BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
            BulkItemResponse[] items = response.getItems();
            if (items.length != pending.size()) {
                throw new IllegalStateException(
                        "Elasticsearch Bulk response item count mismatch: expected="
                                + pending.size() + ", actual=" + items.length);
            }

            List<Document> retryable = new ArrayList<Document>();
            List<String> permanentFailures = new ArrayList<String>();
            for (int index = 0; index < items.length; index++) {
                BulkItemResponse item = items[index];
                if (!item.isFailed()) {
                    continue;
                }
                int status = item.status().getStatus();
                if (isRetryableStatus(status) && retryNumber < config.getMaxRetries()) {
                    retryable.add(pending.get(index));
                } else {
                    permanentFailures.add(
                            "status=" + status
                                    + ", id=" + item.getId()
                                    + ", message=" + item.getFailureMessage());
                }
            }

            if (!permanentFailures.isEmpty()) {
                throw new IllegalStateException(
                        "Elasticsearch Bulk item failure(s): " + permanentFailures);
            }
            if (retryable.isEmpty()) {
                return documents.size();
            }

            long backoff = config.retryBackoffMs(retryNumber);
            retryNumber++;
            sleep(backoff);
            pending = retryable;
        }
    }

    static boolean isRetryableStatus(int status) {
        return status == 408
                || status == 429
                || status == 502
                || status == 503
                || status == 504;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while backing off Elasticsearch Bulk item retry",
                    interrupted);
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
