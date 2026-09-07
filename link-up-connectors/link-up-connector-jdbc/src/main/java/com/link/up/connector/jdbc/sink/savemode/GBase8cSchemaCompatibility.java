package com.link.up.connector.jdbc.sink.savemode;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cCompatibilityMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Logical/physical schema normalization used only while validating GBase 8c targets.
 *
 * <p>In A compatibility mode GBase 8c documents DATE as the physical type
 * {@code TIMESTAMP(0) WITHOUT TIME ZONE}. The generic JDBC metadata mapper therefore reads an
 * auto-created DATE target back as Flux TIMESTAMP. This helper treats that one documented
 * physical representation as logical DATE when the corresponding source column is DATE. It does
 * not globally widen DATE -> TIMESTAMP conversion for other databases or other GBase 8c modes.</p>
 */
public final class GBase8cSchemaCompatibility {

    private GBase8cSchemaCompatibility() {
    }

    /** Returns true only when mode resolution could repair a DATE(source)/TIMESTAMP(target) pair. */
    public static boolean hasDateTimestampCandidate(
            TableSchema sourceSchema,
            TableSchema targetSchema) {
        if (sourceSchema == null || targetSchema == null) {
            return false;
        }
        Map<String, Column> targetByName = columnsByName(targetSchema.getColumns());
        for (Column source : sourceSchema.getColumns()) {
            Column target = targetByName.get(normalize(source.getName()));
            if (target != null
                    && source.getDataType().getSqlType() == SqlType.DATE
                    && target.getDataType().getSqlType() == SqlType.TIMESTAMP) {
                return true;
            }
        }
        return false;
    }

    /**
     * Normalizes only the target columns that correspond to source DATE columns in A mode.
     */
    public static TableSchema normalizeTargetForValidation(
            TableSchema sourceSchema,
            TableSchema targetSchema,
            GBase8cCompatibilityMode compatibilityMode) {
        if (sourceSchema == null || targetSchema == null) {
            throw new IllegalArgumentException("sourceSchema/targetSchema must not be null");
        }
        if (compatibilityMode != GBase8cCompatibilityMode.A) {
            return targetSchema;
        }

        Map<String, Column> sourceByName = columnsByName(sourceSchema.getColumns());
        List<Column> normalized = new ArrayList<Column>(targetSchema.getColumns().size());
        boolean changed = false;
        for (Column target : targetSchema.getColumns()) {
            Column source = sourceByName.get(normalize(target.getName()));
            if (source != null
                    && source.getDataType().getSqlType() == SqlType.DATE
                    && target.getDataType().getSqlType() == SqlType.TIMESTAMP) {
                normalized.add(target.withType(BasicType.DATE_TYPE));
                changed = true;
            } else {
                normalized.add(target);
            }
        }

        if (!changed) {
            return targetSchema;
        }
        return TableSchema.builder()
                .columns(normalized)
                .primaryKey(targetSchema.getPrimaryKey())
                .build();
    }

    private static Map<String, Column> columnsByName(List<Column> columns) {
        Map<String, Column> result = new HashMap<String, Column>();
        if (columns == null) {
            return result;
        }
        for (Column column : columns) {
            result.put(normalize(column.getName()), column);
        }
        return result;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }
}
