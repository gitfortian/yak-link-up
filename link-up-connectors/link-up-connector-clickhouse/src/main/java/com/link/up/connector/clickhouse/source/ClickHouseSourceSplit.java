package com.link.up.connector.clickhouse.source;

import com.link.up.api.source.SourceSplit;
import com.link.up.api.table.catalog.TablePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** One bounded ClickHouse read unit, bound to a concrete HTTP node. */
public final class ClickHouseSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    public enum Mode {
        PARTS,
        TABLE_QUERY,
        SQL_QUERY
    }

    private final String splitId;
    private final TablePath tablePath;
    private final String endpoint;
    private final Mode mode;
    private final List<String> partNames;
    private final String querySql;

    public ClickHouseSourceSplit(
            String splitId,
            TablePath tablePath,
            String endpoint,
            Mode mode,
            List<String> partNames,
            String querySql) {
        this.splitId = Objects.requireNonNull(splitId, "splitId must not be null");
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath must not be null");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
        this.mode = Objects.requireNonNull(mode, "mode must not be null");
        this.partNames =
                Collections.unmodifiableList(
                        partNames == null
                                ? Collections.<String>emptyList()
                                : new ArrayList<String>(partNames));
        this.querySql = querySql;
        if (mode == Mode.PARTS && this.partNames.isEmpty()) {
            throw new IllegalArgumentException("PARTS split requires at least one part name");
        }
        if (mode == Mode.SQL_QUERY && (querySql == null || querySql.trim().isEmpty())) {
            throw new IllegalArgumentException("SQL_QUERY split requires querySql");
        }
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String dataSetId() {
        return tablePath.toString();
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Mode getMode() {
        return mode;
    }

    public List<String> getPartNames() {
        return partNames;
    }

    public String getQuerySql() {
        return querySql;
    }

    @Override
    public String toString() {
        return "ClickHouseSourceSplit{"
                + "splitId='" + splitId + '\''
                + ", tablePath=" + tablePath
                + ", endpoint='" + endpoint + '\''
                + ", mode=" + mode
                + ", parts=" + partNames.size()
                + '}';
    }
}
