package com.link.up.connector.doris.client.source;

import com.link.up.connector.doris.client.source.model.DorisQueryPartition;
import com.link.up.connector.doris.client.source.model.DorisQueryPlan;
import com.link.up.connector.doris.client.source.model.DorisTablet;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DorisSplitPlannerTest {

    @Test
    public void balancesTabletRoutingAndChunksDeterministically() {
        DorisQueryPlan plan = new DorisQueryPlan();
        plan.setStatus(200);
        plan.setOpaquedQueryPlan("opaque");
        Map<String, DorisTablet> tablets = new LinkedHashMap<String, DorisTablet>();
        tablets.put("3", tablet("be-b:9060", "be-a:9060"));
        tablets.put("1", tablet("be-a:9060", "be-b:9060"));
        tablets.put("2", tablet("be-a:9060", "be-b:9060"));
        tablets.put("4", tablet("be-a:9060", "be-b:9060"));
        plan.setPartitions(tablets);

        List<DorisQueryPartition> partitions =
                DorisSplitPlanner.plan("db", "orders", plan, 1);

        assertEquals(4, partitions.size());
        assertEquals("be-a:9060", partitions.get(0).getBeAddress());
        assertEquals(Arrays.asList(1L), partitions.get(0).getTabletIds());
        assertEquals(Arrays.asList(3L), partitions.get(1).getTabletIds());
        assertEquals("be-b:9060", partitions.get(2).getBeAddress());
        assertEquals(Arrays.asList(2L), partitions.get(2).getTabletIds());
        assertEquals(Arrays.asList(4L), partitions.get(3).getTabletIds());
    }

    private static DorisTablet tablet(String... routings) {
        DorisTablet tablet = new DorisTablet();
        tablet.setRoutings(Arrays.asList(routings));
        return tablet;
    }
}
