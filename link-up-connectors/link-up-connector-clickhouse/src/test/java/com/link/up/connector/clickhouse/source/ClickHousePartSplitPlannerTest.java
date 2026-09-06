package com.link.up.connector.clickhouse.source;

import com.link.up.api.table.catalog.TablePath;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ClickHousePartSplitPlannerTest {

    @Test
    public void sortsAndChunksPartsDeterministically() {
        List<ClickHouseSourceSplit> splits =
                ClickHousePartSplitPlanner.plan(
                        TablePath.of("analytics", "orders"),
                        "node-1:8123",
                        Arrays.asList("p3", "p1", "p2"),
                        2,
                        0);

        assertEquals(2, splits.size());
        assertEquals(Arrays.asList("p1", "p2"), splits.get(0).getPartNames());
        assertEquals(Arrays.asList("p3"), splits.get(1).getPartNames());
        assertEquals("analytics.orders@node-1:8123#0", splits.get(0).splitId());
        assertEquals(ClickHouseSourceSplit.Mode.PARTS, splits.get(0).getMode());
    }

    @Test
    public void respectsSplitIndexOffset() {
        List<ClickHouseSourceSplit> splits =
                ClickHousePartSplitPlanner.plan(
                        TablePath.of("analytics", "orders"),
                        "node-2:8123",
                        Arrays.asList("part-a"),
                        1,
                        7);

        assertEquals("analytics.orders@node-2:8123#7", splits.get(0).splitId());
    }
}
