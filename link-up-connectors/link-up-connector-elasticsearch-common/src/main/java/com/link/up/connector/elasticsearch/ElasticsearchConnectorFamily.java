package com.link.up.connector.elasticsearch;

/**
 * Stable identifiers shared by the versioned Elasticsearch connector modules.
 *
 * <p>This class intentionally contains no Elasticsearch SDK types. The common module is the
 * product-semantic boundary shared by Elasticsearch 7 and 8, not a runtime connector itself.
 */
public final class ElasticsearchConnectorFamily {

    public static final String FAMILY_IDENTIFIER = "elasticsearch";
    public static final String ELASTICSEARCH7_IDENTIFIER = "elasticsearch7";
    public static final String ELASTICSEARCH8_IDENTIFIER = "elasticsearch8";

    private ElasticsearchConnectorFamily() {
    }
}
