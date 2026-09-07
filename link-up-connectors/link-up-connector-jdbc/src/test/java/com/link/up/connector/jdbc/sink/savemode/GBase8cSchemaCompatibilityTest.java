package com.link.up.connector.jdbc.sink.savemode;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.SqlType;
import com.link.up.connector.jdbc.core.dialect.gbase.gbase8c.GBase8cCompatibilityMode;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GBase8cSchemaCompatibilityTest {

    @Test
    public void detectsA-modeDatePhysicalTimestampCandidateCaseInsensitively() {
        TableSchema source = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("Business_Date", BasicType.DATE_TYPE).build(),
                        Column.builder("id", BasicType.LONG_TYPE).build()))
                .build();
        TableSchema target = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("business_date", BasicType.TIMESTAMP_TYPE).build(),
                        Column.builder("id", BasicType.LONG_TYPE).build()))
                .build();

        assertTrue(GBase8cSchemaCompatibility.hasDateTimestampCandidate(source, target));
    }

    @Test
    public void aModeNormalizesOnlyMatchingDateTimestampColumns() {
        TableSchema source = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("business_date", BasicType.DATE_TYPE).build(),
                        Column.builder("updated_at", BasicType.TIMESTAMP_TYPE).build()))
                .build();
        TableSchema target = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("business_date", BasicType.TIMESTAMP_TYPE).build(),
                        Column.builder("updated_at", BasicType.TIMESTAMP_TYPE).build()))
                .build();

        TableSchema normalized = GBase8cSchemaCompatibility.normalizeTargetForValidation(
                source,
                target,
                GBase8cCompatibilityMode.A);

        assertEquals(SqlType.DATE, normalized.getColumns().get(0).getDataType().getSqlType());
        assertEquals(
                SqlType.TIMESTAMP,
                normalized.getColumns().get(1).getDataType().getSqlType());
    }

    @Test
    public void nonAModesDoNotRelaxDateTimestampValidation() {
        TableSchema source = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("business_date", BasicType.DATE_TYPE).build()))
                .build();
        TableSchema target = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("business_date", BasicType.TIMESTAMP_TYPE).build()))
                .build();

        assertSame(
                target,
                GBase8cSchemaCompatibility.normalizeTargetForValidation(
                        source,
                        target,
                        GBase8cCompatibilityMode.PG));
        assertSame(
                target,
                GBase8cSchemaCompatibility.normalizeTargetForValidation(
                        source,
                        target,
                        GBase8cCompatibilityMode.B));
    }

    @Test
    public void ordinaryCompatibleSchemasDoNotRequireModeResolutionCandidate() {
        TableSchema source = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("id", BasicType.LONG_TYPE).build(),
                        Column.builder("created_at", BasicType.TIMESTAMP_TYPE).build()))
                .build();
        TableSchema target = TableSchema.builder()
                .columns(Arrays.asList(
                        Column.builder("id", BasicType.LONG_TYPE).build(),
                        Column.builder("created_at", BasicType.TIMESTAMP_TYPE).build()))
                .build();

        assertFalse(GBase8cSchemaCompatibility.hasDateTimestampCandidate(source, target));
    }
}
