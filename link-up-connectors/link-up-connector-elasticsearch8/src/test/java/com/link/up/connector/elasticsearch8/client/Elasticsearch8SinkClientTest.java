package com.link.up.connector.elasticsearch8.client;

import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Elasticsearch8SinkClientTest {

    @Test
    public void classifiesRetryableStatuses() {
        assertTrue(Elasticsearch8SinkClient.isRetryableStatus(408));
        assertTrue(Elasticsearch8SinkClient.isRetryableStatus(429));
        assertTrue(Elasticsearch8SinkClient.isRetryableStatus(502));
        assertTrue(Elasticsearch8SinkClient.isRetryableStatus(503));
        assertTrue(Elasticsearch8SinkClient.isRetryableStatus(504));
        assertFalse(Elasticsearch8SinkClient.isRetryableStatus(400));
        assertFalse(Elasticsearch8SinkClient.isRetryableStatus(409));
    }

    @Test
    public void treatsErrorOrHttpFailureAsItemFailure() {
        BulkResponseItem success = BulkResponseItem.of(item -> item
                .operationType(OperationType.Index)
                .index("orders")
                .id("1")
                .status(201));
        BulkResponseItem failed = BulkResponseItem.of(item -> item
                .operationType(OperationType.Index)
                .index("orders")
                .id("2")
                .status(429)
                .error(error -> error.reason("too many requests")));

        assertFalse(Elasticsearch8SinkClient.isFailure(success));
        assertTrue(Elasticsearch8SinkClient.isFailure(failed));
    }
}
