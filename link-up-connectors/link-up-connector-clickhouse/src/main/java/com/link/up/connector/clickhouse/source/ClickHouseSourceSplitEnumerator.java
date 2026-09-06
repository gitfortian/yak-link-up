package com.link.up.connector.clickhouse.source;

import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.connector.clickhouse.client.ClickHouseJdbcClient;
import com.link.up.connector.clickhouse.config.ClickHouseSourceConfig;
import com.link.up.connector.clickhouse.config.ClickHouseSourceTableConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** Bounded split enumerator using ClickHouse active parts for MergeTree tables. */
public final class ClickHouseSourceSplitEnumerator
        implements SourceSplitEnumerator<ClickHouseSourceSplit> {

    private final ClickHouseSourceConfig config;
    private final ClickHouseJdbcClient client;

    public ClickHouseSourceSplitEnumerator(ClickHouseSourceConfig config) {
        this(config, new ClickHouseJdbcClient(config));
    }

    ClickHouseSourceSplitEnumerator(
            ClickHouseSourceConfig config,
            ClickHouseJdbcClient client) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public List<ClickHouseSourceSplit> enumerateSplits() throws Exception {
        List<ClickHouseSourceSplit> result = new ArrayList<ClickHouseSourceSplit>();
        for (ClickHouseSourceTableConfig tableConfig : config.getTableConfigs()) {
            if (tableConfig.isSqlMode()) {
                String endpoint = config.getHosts().get(0);
                result.add(
                        new ClickHouseSourceSplit(
                                tableConfig.getTablePath() + "@" + endpoint + "#sql",
                                tableConfig.getTablePath(),
                                endpoint,
                                ClickHouseSourceSplit.Mode.SQL_QUERY,
                                Collections.<String>emptyList(),
                                tableConfig.getSql()));
                continue;
            }

            int splitIndex = 0;
            boolean distributed = false;
            for (String endpoint : config.getHosts()) {
                String engine = client.getTableEngine(endpoint, tableConfig);
                String normalized = engine == null ? "" : engine.toLowerCase(Locale.ROOT);
                if (normalized.contains("mergetree")) {
                    List<String> parts = client.listActiveParts(endpoint, tableConfig);
                    List<ClickHouseSourceSplit> planned =
                            ClickHousePartSplitPlanner.plan(
                                    tableConfig.getTablePath(),
                                    endpoint,
                                    parts,
                                    tableConfig.getSplitSize(),
                                    splitIndex);
                    result.addAll(planned);
                    splitIndex += planned.size();
                    continue;
                }

                if ("distributed".equalsIgnoreCase(engine)) {
                    if (!tableConfig.getPartitionList().isEmpty()) {
                        throw new IllegalArgumentException(
                                "partition_list is not supported for ClickHouse Distributed table mode in this stage; "
                                        + "use filter_query/sql or read the local MergeTree table explicitly");
                    }
                    String firstEndpoint = config.getHosts().get(0);
                    result.add(
                            new ClickHouseSourceSplit(
                                    tableConfig.getTablePath()
                                            + "@"
                                            + firstEndpoint
                                            + "#distributed",
                                    tableConfig.getTablePath(),
                                    firstEndpoint,
                                    ClickHouseSourceSplit.Mode.TABLE_QUERY,
                                    Collections.<String>emptyList(),
                                    null));
                    distributed = true;
                    break;
                }

                throw new IllegalArgumentException(
                        "ClickHouse table mode requires a MergeTree-family or Distributed table, but "
                                + tableConfig.getTablePath()
                                + " uses engine="
                                + engine
                                + ". Configure sql for this table instead.");
            }
            if (distributed) {
                continue;
            }
        }
        return Collections.unmodifiableList(result);
    }

    @Override
    public void close() {
        client.close();
    }
}
