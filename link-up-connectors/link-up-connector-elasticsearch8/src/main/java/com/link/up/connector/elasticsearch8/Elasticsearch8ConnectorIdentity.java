package com.link.up.connector.elasticsearch8;

import com.link.up.connector.elasticsearch.ElasticsearchConnectorFamily;

/** Stage 0 identity boundary for the Elasticsearch 8 connector. */
public final class Elasticsearch8ConnectorIdentity {

    public static final String IDENTIFIER =
            ElasticsearchConnectorFamily.ELASTICSEARCH8_IDENTIFIER;
    public static final int MAJOR_VERSION = 8;

    private Elasticsearch8ConnectorIdentity() {
    }
}
