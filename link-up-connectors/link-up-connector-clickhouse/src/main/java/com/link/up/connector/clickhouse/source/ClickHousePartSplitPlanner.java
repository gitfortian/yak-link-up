package com.link.up.connector.clickhouse.source;

import com.link.up.api.table.catalog.TablePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Deterministically chunks ClickHouse active part names into Link-Up splits. */
public final class ClickHousePartSplitPlanner {

    private ClickHousePartSplitPlanner() {
    }

    public static List<ClickHouseSourceSplit> plan(
            TablePath tablePath,
            String endpoint,
            List<String> partNames,
            int splitSize,
            int splitIndexStart) {
        if (tablePath == null) {
            throw new IllegalArgumentException("tablePath must not be null");
        }
        if (endpoint == null || endpoint.trim().isEmpty()) {
            throw new IllegalArgumentException("endpoint must not be empty");
        }
        if (splitSize <= 0) {
            throw new IllegalArgumentException("splitSize must be greater than 0");
        }
        if (partNames == null || partNames.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> ordered = new ArrayList<String>();
        for (String partName : partNames) {
            if (partName == null || partName.trim().isEmpty()) {
                throw new IllegalArgumentException("ClickHouse part name must not be empty");
            }
            ordered.add(partName.trim());
        }
        Collections.sort(ordered);

        List<ClickHouseSourceSplit> result = new ArrayList<ClickHouseSourceSplit>();
        int offset = 0;
        int splitIndex = splitIndexStart;
        while (offset < ordered.size()) {
            int end = Math.min(ordered.size(), offset + splitSize);
            List<String> parts = new ArrayList<String>(ordered.subList(offset, end));
            String splitId =
                    tablePath.toString()
                            + "@"
                            + endpoint
                            + "#"
                            + splitIndex++;
            result.add(
                    new ClickHouseSourceSplit(
                            splitId,
                            tablePath,
                            endpoint,
                            ClickHouseSourceSplit.Mode.PARTS,
                            parts,
                            null));
            offset = end;
        }
        return Collections.unmodifiableList(result);
    }
}
