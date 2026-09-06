package com.link.up.connector.elasticsearch7;

import com.link.up.connector.elasticsearch.ElasticsearchConnectorFamily;

/** Stage 0 identity boundary for the Elasticsearch 7 connector. */
public final class Elasticsearch7ConnectorIdentity {

    public static final String IDENTIFIER =
            ElasticsearchConnectorFamily.ELASTICSEARCH7_IDENTIFIER;
    public static final int MAJOR_VERSION = 7;

    private Elasticsearch7ConnectorIdentity() {
    }
}
