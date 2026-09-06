package com.link.up.connector.doris.source;

import com.link.up.api.source.SourceSplitEnumerator;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.connector.doris.client.source.DorisQueryPlanClient;
import com.link.up.connector.doris.client.source.DorisSplitPlanner;
import com.link.up.connector.doris.client.source.model.DorisQueryPartition;
import com.link.up.connector.doris.client.source.model.DorisQueryPlan;
import com.link.up.connector.doris.config.DorisSourceConfig;
import com.link.up.connector.doris.config.DorisSourceTableConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Bounded native split enumerator backed by Doris FE query plans. */
public final class DorisSourceSplitEnumerator
        implements SourceSplitEnumerator<DorisSourceSplit> {

    private final DorisSourceConfig config;
    private final Map<TablePath, CatalogTable> tables;
    private final DorisQueryPlanClient queryPlanClient;

    public DorisSourceSplitEnumerator(
            DorisSourceConfig config,
            Map<TablePath, CatalogTable> tables) {
        this(config, tables, new DorisQueryPlanClient(config));
    }

    DorisSourceSplitEnumerator(
            DorisSourceConfig config,
            Map<TablePath, CatalogTable> tables,
            DorisQueryPlanClient queryPlanClient) {
        this.config = config;
        this.tables = tables;
        this.queryPlanClient = queryPlanClient;
    }

    @Override
    public List<DorisSourceSplit> enumerateSplits() throws Exception {
        List<DorisSourceSplit> result = new ArrayList<DorisSourceSplit>();
        for (DorisSourceTableConfig tableConfig : config.getTableConfigs()) {
            CatalogTable table = tables.get(tableConfig.getTablePath());
            if (table == null) {
                throw new IllegalArgumentException(
                        "Cannot find discovered schema for Doris source table: "
                                + tableConfig.getTablePath());
            }
            DorisQueryPlan queryPlan = queryPlanClient.fetchQueryPlan(tableConfig, table);
            List<DorisQueryPartition> partitions =
                    DorisSplitPlanner.plan(
                            tableConfig.getDatabase(),
                            tableConfig.getTable(),
                            queryPlan,
                            tableConfig.getRequestTabletSize());
            int partitionIndex = 0;
            for (DorisQueryPartition partition : partitions) {
                String splitId =
                        tableConfig.getDatabase()
                                + "."
                                + tableConfig.getTable()
                                + "@"
                                + partition.getBeAddress()
                                + "#"
                                + partitionIndex++;
                result.add(
                        new DorisSourceSplit(
                                splitId,
                                tableConfig.getTablePath(),
                                partition));
            }
        }
        return Collections.unmodifiableList(result);
    }

    @Override
    public void close() {
        queryPlanClient.close();
    }
}
