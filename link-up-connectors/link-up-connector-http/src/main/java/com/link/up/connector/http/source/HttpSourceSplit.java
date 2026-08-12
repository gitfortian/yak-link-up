package com.link.up.connector.http.source;

import com.link.up.api.source.SourceSplit;

/**
 * HTTP Source 数据分片。
 *
 * <p>HTTP Source 通常只有一个分片，
 * 分页逻辑由 Reader 内部处理。
 */
public final class HttpSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private static final String SPLIT_ID = "http-split-0";
    private static final String DATASET_ID = "http";

    @Override
    public String splitId() {
        return SPLIT_ID;
    }

    @Override
    public String dataSetId() {
        return DATASET_ID;
    }

    @Override
    public String toString() {
        return "HttpSourceSplit{splitId='" + SPLIT_ID + "'}";
    }
}
