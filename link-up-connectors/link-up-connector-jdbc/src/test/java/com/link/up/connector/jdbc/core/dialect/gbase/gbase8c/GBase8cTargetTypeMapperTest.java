package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GBase8cTargetTypeMapperTest {

    @Test
    public void pgModeMayPreserveBoundedVarcharLength() {
        Column column = Column.builder("name", BasicType.STRING_TYPE)
                .length(120L)
                .build();

        assertEquals(
                "VARCHAR(120)",
                mapper(GBase8cCompatibilityMode.PG).toDatabaseType(column));
    }

    @Test
    public void nonPgModesUseTextToAvoidCharacterVsByteLengthGuessing() {
        Column column = Column.builder("name", BasicType.STRING_TYPE)
                .length(120L)
                .build();

        assertEquals("TEXT", mapper(GBase8cCompatibilityMode.A).toDatabaseType(column));
        assertEquals("TEXT", mapper(GBase8cCompatibilityMode.B).toDatabaseType(column));
        assertEquals("TEXT", mapper(GBase8cCompatibilityMode.C).toDatabaseType(column));
    }

    @Test
    public void oracleCompatibleDateUsesDocumentedPhysicalTimestampContract() {
        Column date = Column.builder("biz_date", BasicType.DATE_TYPE).build();

        assertEquals(
                "TIMESTAMP(0) WITHOUT TIME ZONE",
                mapper(GBase8cCompatibilityMode.A).toDatabaseType(date));
        assertEquals("DATE", mapper(GBase8cCompatibilityMode.B).toDatabaseType(date));
        assertEquals("DATE", mapper(GBase8cCompatibilityMode.C).toDatabaseType(date));
        assertEquals("DATE", mapper(GBase8cCompatibilityMode.PG).toDatabaseType(date));
    }

    @Test
    public void portableCoreScalarTypesRoundTripAcrossModes() {
        for (GBase8cCompatibilityMode mode : GBase8cCompatibilityMode.values()) {
            GBase8cTargetTypeMapper mapper = mapper(mode);
            assertEquals(
                    "SMALLINT",
                    mapper.toDatabaseType(
                            Column.builder("tiny", BasicType.BYTE_TYPE).build()));
            assertEquals(
                    "INTEGER",
                    mapper.toDatabaseType(
                            Column.builder("id", BasicType.INT_TYPE).build()));
            assertEquals(
                    "BIGINT",
                    mapper.toDatabaseType(
                            Column.builder("big_id", BasicType.LONG_TYPE).build()));
            assertEquals(
                    "REAL",
                    mapper.toDatabaseType(
                            Column.builder("ratio", BasicType.FLOAT_TYPE).build()));
            assertEquals(
                    "DOUBLE PRECISION",
                    mapper.toDatabaseType(
                            Column.builder("score", BasicType.DOUBLE_TYPE).build()));
            assertEquals(
                    "NUMERIC(20,4)",
                    mapper.toDatabaseType(
                            Column.builder("amount", new DecimalType(20, 4))
                                    .precision(20)
                                    .scale(4)
                                    .build()));
            assertEquals(
                    "BYTEA",
                    mapper.toDatabaseType(
                            Column.builder("payload", BasicType.BYTES_TYPE).build()));
            assertEquals(
                    "TIMESTAMP",
                    mapper.toDatabaseType(
                            Column.builder("created_at", BasicType.TIMESTAMP_TYPE).build()));
        }
    }

    @Test
    public void timezoneTimestampIsOnlyEnabledForVerifiedPgMode() {
        Column column = Column.builder("event_at", BasicType.TIMESTAMP_TZ_TYPE).build();

        assertEquals(
                "TIMESTAMP WITH TIME ZONE",
                mapper(GBase8cCompatibilityMode.PG).toDatabaseType(column));

        for (GBase8cCompatibilityMode mode : new GBase8cCompatibilityMode[]{
                GBase8cCompatibilityMode.A,
                GBase8cCompatibilityMode.B,
                GBase8cCompatibilityMode.C}) {
            UnsupportedOperationException error = assertThrows(
                    UnsupportedOperationException.class,
                    () -> mapper(mode).toDatabaseType(column));
            assertTrue(error.getMessage().contains("TIMESTAMP_TZ"));
            assertTrue(error.getMessage().contains(mode.databaseValue()));
        }
    }

    @Test
    public void modeLessJdbcTypeMapperEntryPointRemainsBlocked() {
        GBase8cTypeMapper mapper = new GBase8cTypeMapper();
        Column column = Column.builder("id", BasicType.INT_TYPE).build();

        assertThrows(
                UnsupportedOperationException.class,
                () -> mapper.toDatabaseType(column));
        assertEquals(
                "INTEGER",
                mapper.toDatabaseType(column, GBase8cCompatibilityMode.PG));
    }

    private static GBase8cTargetTypeMapper mapper(GBase8cCompatibilityMode mode) {
        return new GBase8cTargetTypeMapper(mode);
    }
}
