package com.link.up.connector.elasticsearch8.source;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.connector.elasticsearch8.config.Elasticsearch8SourceConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Elasticsearch8SourceSplitEnumeratorTest {

    @Test
    public void usesRuntimeParallelismWhenSlicesAreNotFixed() throws Exception {
        Elasticsearch8SourceConfig config = config(null);
        List<Elasticsearch8SourceSplit> splits =
                new Elasticsearch8SourceSplitEnumerator(config, 3).enumerateSplits();
        assertEquals(3, splits.size());
        assertEquals(0, splits.get(0).getSliceId());
        assertEquals(3, splits.get(0).getSliceMax());
    }

    @Test
    public void usesConfiguredSliceCount() throws Exception {
        Elasticsearch8SourceConfig config = config(4);
        List<Elasticsearch8SourceSplit> splits =
                new Elasticsearch8SourceSplitEnumerator(config, 2).enumerateSplits();
        assertEquals(4, splits.size());
        assertEquals("orders#slice-3-of-4", splits.get(3).splitId());
    }

    private static Elasticsearch8SourceConfig config(Integer slices) {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("hosts", Arrays.asList("http://localhost:9200"));
        values.put("index", "orders");
        if (slices != null) {
            values.put("slices", slices);
        }
        return Elasticsearch8SourceConfig.of(ReadonlyConfig.fromMap(values));
    }
}
