package com.link.up.connector.elasticsearch8.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.InfoResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.connector.elasticsearch8.Elasticsearch8ConnectorIdentity;
import com.link.up.connector.elasticsearch8.config.Elasticsearch8SinkConfig;
import com.link.up.connector.elasticsearch8.converter.Elasticsearch8DocumentConverter.Document;
import com.link.up.connector.elasticsearch8.schema.Elasticsearch8TypeMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** ES8 Java API Client boundary for target discovery and synchronous bounded Bulk writes. */
public final class Elasticsearch8SinkClient implements AutoCloseable {

    private final Elasticsearch8SinkConfig config;
    private final Elasticsearch8ClientResources resources;
    private final ElasticsearchClient client;

    public Elasticsearch8SinkClient(Elasticsearch8SinkConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.resources = Elasticsearch8ClientResources.create(
                config.getHosts(),
                config.getUsername(),
                config.getPassword(),
                config.getConnectTimeoutMs(),
                config.getSocketTimeoutMs());
        this.client = resources.getClient();
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

    public CatalogTable discoverTargetTable(List<String> projectedFields) throws IOException {
        verifyMajorVersion();
        GetMappingResponse response =
                client.indices().getMapping(request -> request.index(config.getIndex()));
        Map<String, IndexMappingRecord> mappings = response.result();
        if (mappings == null || mappings.isEmpty()) {
            throw new IllegalArgumentException(
                    "Elasticsearch 8 Sink target index must already exist and contain a mapping: "
                            + config.getIndex());
        }

        String resolvedIndex = config.getIndex();
        IndexMappingRecord record = mappings.get(config.getIndex());
        if (record == null) {
            if (mappings.size() != 1) {
                throw new IllegalArgumentException(
                        "Stage 4 Elasticsearch 8 Sink requires one concrete target index; alias '"
                                + config.getIndex() + "' resolved to " + mappings.keySet());
            }
            Map.Entry<String, IndexMappingRecord> only = mappings.entrySet().iterator().next();
            resolvedIndex = only.getKey();
            record = only.getValue();
        }

        TableSchema schema =
                Elasticsearch8TypeMapper.toTableSchema(record.mappings(), projectedFields);
        return CatalogTable.builder(TablePath.of(config.getIndex()), schema)
                .option("resolved_index", resolvedIndex)
                .build();
    }

    /**
     * Indexes all supplied documents. Only explicit retryable item failures are retried.
     * Whole-request exceptions are deliberately propagated without retry because server-side
     * success is ambiguous at that boundary.
     */
    public int bulkIndex(List<Document> documents) throws IOException {
        Objects.requireNonNull(documents, "documents must not be null");
        if (documents.isEmpty()) {
            return 0;
        }

        List<Document> pending = new ArrayList<Document>(documents);
        int retryNumber = 0;
        while (true) {
            BulkRequest.Builder request = new BulkRequest.Builder();
            for (Document document : pending) {
                IndexOperation<Map<String, Object>> operation =
                        IndexOperation.of(index -> index
                                .index(config.getIndex())
                                .id(document.getId())
                                .document(document.getSource()));
                request.operations(operation);
            }

            BulkResponse response = client.bulk(request.build());
            List<BulkResponseItem> items = response.items();
            if (items.size() != pending.size()) {
                throw new IllegalStateException(
                        "Elasticsearch Bulk response item count mismatch: expected="
                                + pending.size() + ", actual=" + items.size());
            }

            List<Document> retryable = new ArrayList<Document>();
            List<String> permanentFailures = new ArrayList<String>();
            for (int index = 0; index < items.size(); index++) {
                BulkResponseItem item = items.get(index);
                if (!isFailure(item)) {
                    continue;
                }
                int status = item.status();
                if (isRetryableStatus(status) && retryNumber < config.getMaxRetries()) {
                    retryable.add(pending.get(index));
                } else {
                    ErrorCause error = item.error();
                    permanentFailures.add(
                            "status=" + status
                                    + ", id=" + item.id()
                                    + ", message="
                                    + (error == null ? "bulk item failed" : error.reason()));
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

    static boolean isFailure(BulkResponseItem item) {
        return item.error() != null || item.status() >= 300;
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
        resources.close();
    }
}
