package com.link.up.connector.elasticsearch7.source;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.connector.elasticsearch7.config.Elasticsearch7SourceConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Elasticsearch7SourceSplitEnumeratorTest {

    @Test
    public void defaultsToReaderParallelism() throws Exception {
        Elasticsearch7SourceConfig config = config(null);
        List<Elasticsearch7SourceSplit> splits =
                new Elasticsearch7SourceSplitEnumerator(config, 3).enumerateSplits();
        assertEquals(3, splits.size());
        assertEquals("orders:slice-0-of-3", splits.get(0).splitId());
        assertEquals(2, splits.get(2).getSliceId());
        assertEquals(3, splits.get(2).getSliceMax());
    }

    @Test
    public void fixedSlicesStayStableAcrossRuntimeParallelism() throws Exception {
        Elasticsearch7SourceConfig config = config(2);
        List<Elasticsearch7SourceSplit> splits =
                new Elasticsearch7SourceSplitEnumerator(config, 8).enumerateSplits();
        assertEquals(2, splits.size());
        assertEquals("orders:slice-1-of-2", splits.get(1).splitId());
    }

    private static Elasticsearch7SourceConfig config(Integer slices) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("hosts", Arrays.asList("http://localhost:9200"));
        values.put("index", "orders");
        if (slices != null) {
            values.put("slices", slices);
        }
        return Elasticsearch7SourceConfig.of(ReadonlyConfig.fromMap(values));
    }
}
