package com.link.up.connector.elasticsearch7.client;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Elasticsearch7SinkClientTest {

    @Test
    public void retryPolicyOnlyCoversExplicitTransientItemStatuses() {
        assertTrue(Elasticsearch7SinkClient.isRetryableStatus(408));
        assertTrue(Elasticsearch7SinkClient.isRetryableStatus(429));
        assertTrue(Elasticsearch7SinkClient.isRetryableStatus(502));
        assertTrue(Elasticsearch7SinkClient.isRetryableStatus(503));
        assertTrue(Elasticsearch7SinkClient.isRetryableStatus(504));
        assertFalse(Elasticsearch7SinkClient.isRetryableStatus(400));
        assertFalse(Elasticsearch7SinkClient.isRetryableStatus(409));
    }
}
