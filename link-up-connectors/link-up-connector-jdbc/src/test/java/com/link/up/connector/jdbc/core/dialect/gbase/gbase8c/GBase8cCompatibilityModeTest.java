package com.link.up.connector.jdbc.core.dialect.gbase.gbase8c;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class GBase8cCompatibilityModeTest {

    @Test
    public void parsesStableDatabaseCompatibilityValues() {
        assertEquals(
                GBase8cCompatibilityMode.A,
                GBase8cCompatibilityMode.fromDatabaseValue("A"));
        assertEquals(
                GBase8cCompatibilityMode.B,
                GBase8cCompatibilityMode.fromDatabaseValue("b"));
        assertEquals(
                GBase8cCompatibilityMode.C,
                GBase8cCompatibilityMode.fromDatabaseValue(" C "));
        assertEquals(
                GBase8cCompatibilityMode.PG,
                GBase8cCompatibilityMode.fromDatabaseValue("pg"));
    }

    @Test
    public void exposesCompatibleProductNames() {
        assertEquals("oracle", GBase8cCompatibilityMode.A.compatibleProduct());
        assertEquals("mysql", GBase8cCompatibilityMode.B.compatibleProduct());
        assertEquals("teradata", GBase8cCompatibilityMode.C.compatibleProduct());
        assertEquals("postgresql", GBase8cCompatibilityMode.PG.compatibleProduct());
    }

    @Test
    public void rejectsUnknownOrFutureModeInsteadOfAssumingPostgresql() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GBase8cCompatibilityMode.fromDatabaseValue("MSSQL"));

        assertTrue(error.getMessage().contains("A, B, C and PG"));
        assertThrows(
                IllegalArgumentException.class,
                () -> GBase8cCompatibilityMode.fromDatabaseValue(" "));
    }
}
