package com.link.up.connector.mongodb.source;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.connector.mongodb.config.MongoSourceConfig;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MongoSourceSplitEnumeratorTest {

    @Test
    public void stage2UsesOneBoundedCollectionSplit() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("uri", "mongodb://localhost:27017/app");
        values.put("collection", "users");

        MongoSourceConfig config = MongoSourceConfig.of(ReadonlyConfig.fromMap(values));
        List<MongoSourceSplit> splits = new MongoSourceSplitEnumerator(config).enumerateSplits();

        assertEquals(1, splits.size());
        assertEquals("app.users", splits.get(0).dataSetId());
        assertEquals("app.users:full-scan", splits.get(0).splitId());
    }
}
