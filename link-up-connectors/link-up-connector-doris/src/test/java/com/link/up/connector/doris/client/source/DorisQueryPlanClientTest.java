package com.link.up.connector.doris.client.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.connector.doris.client.source.model.DorisQueryPlan;
import com.link.up.connector.doris.config.DorisSourceTableConfig;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DorisQueryPlanClientTest {

    @Test
    public void buildsProjectedBoundedQuerySql() {
        DorisSourceTableConfig config =
                new DorisSourceTableConfig(
                        "analytics",
                        "orders",
                        "id >= 100",
                        Arrays.asList("id", "amount"),
                        Integer.MAX_VALUE,
                        1024,
                        2_147_483_648L);
        TableSchema schema =
                TableSchema.builder()
                        .column(Column.builder("id", BasicType.LONG_TYPE).build())
                        .column(Column.builder("amount", BasicType.DOUBLE_TYPE).build())
                        .build();
        CatalogTable table = CatalogTable.builder(TablePath.of("analytics", "orders"), schema).build();

        assertEquals(
                "SELECT `id`, `amount` FROM `analytics`.`orders` WHERE id >= 100",
                DorisQueryPlanClient.buildQuerySql(config, table));
    }

    @Test
    public void parsesLegacyQueryPlanResponse() throws Exception {
        String json =
                "{\"status\":200,\"opaqued_query_plan\":\"opaque\","
                        + "\"partitions\":{\"10\":{\"routings\":[\"be-1:9060\"]}}}";
        DorisQueryPlan plan =
                DorisQueryPlanClient.parsePlanResponse(json, new ObjectMapper());
        assertEquals(200, plan.getStatus());
        assertEquals("opaque", plan.getOpaquedQueryPlan());
        assertTrue(plan.getPartitions().containsKey("10"));
    }

    @Test
    public void unwrapsDorisV2ResponseEnvelope() throws Exception {
        String json =
                "{\"msg\":\"success\",\"code\":0,\"count\":0,\"data\":{"
                        + "\"status\":200,\"opaqued_query_plan\":\"opaque-v2\","
                        + "\"partitions\":{\"11\":{\"routings\":[\"be-2:9060\"]}}}}";
        DorisQueryPlan plan =
                DorisQueryPlanClient.parsePlanResponse(json, new ObjectMapper());
        assertEquals(200, plan.getStatus());
        assertEquals("opaque-v2", plan.getOpaquedQueryPlan());
        assertEquals("be-2:9060", plan.getPartitions().get("11").getRoutings().get(0));
    }

    @Test(expected = java.io.IOException.class)
    public void rejectsFailedV2Envelope() throws Exception {
        DorisQueryPlanClient.parsePlanResponse(
                "{\"msg\":\"denied\",\"code\":1,\"data\":null}",
                new ObjectMapper());
    }
}
