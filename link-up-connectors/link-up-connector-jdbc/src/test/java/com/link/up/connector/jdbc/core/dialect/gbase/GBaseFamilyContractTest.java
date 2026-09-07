package com.link.up.connector.jdbc.core.dialect.gbase;

import com.link.up.connector.jdbc.core.dialect.DatabaseIdentifier;
import com.link.up.connector.jdbc.core.dialect.gbase.common.GBaseDialectSupport;
import com.link.up.connector.jdbc.core.dialect.gbase.common.GBaseProduct;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GBaseFamilyContractTest {

    @Test
    public void exposesThreeStableFirstClassDatabaseIdentities() {
        assertEquals("gbase8c", DatabaseIdentifier.GBASE8C);
        assertEquals("gbase8a", DatabaseIdentifier.GBASE8A);
        assertEquals("gbase8s", DatabaseIdentifier.GBASE8S);

        Set<String> identifiers =
                Arrays.stream(GBaseProduct.values())
                        .map(GBaseProduct::identifier)
                        .collect(Collectors.toSet());

        assertEquals(
                new HashSet<String>(
                        Arrays.asList(
                                DatabaseIdentifier.GBASE8C,
                                DatabaseIdentifier.GBASE8A,
                                DatabaseIdentifier.GBASE8S)),
                identifiers);
    }

    @Test
    public void familyHelperRecognizesOnlyConcreteProducts() {
        assertTrue(GBaseDialectSupport.isGBase("gbase8c"));
        assertTrue(GBaseDialectSupport.isGBase(" GBASE8A "));
        assertTrue(GBaseDialectSupport.isGBase("gbase8s"));

        assertFalse(GBaseDialectSupport.isGBase("gbase"));
        assertFalse(GBaseDialectSupport.isGBase("gbase8"));
        assertFalse(GBaseDialectSupport.isGBase("mysql"));
        assertFalse(GBaseDialectSupport.isGBase(null));
    }

    @Test
    public void productMetadataKeepsUserFacingNamesSeparate() {
        assertEquals(
                "GBase 8c",
                GBaseProduct.GBASE8C.displayName());
        assertEquals(
                "GBase 8a",
                GBaseProduct.GBASE8A.displayName());
        assertEquals(
                "GBase 8s",
                GBaseProduct.GBASE8S.displayName());

        assertEquals(
                GBaseProduct.GBASE8C,
                GBaseDialectSupport.productOf("GBASE8C").get());
    }

    @Test
    public void databaseIdentifiersDoNotIntroduceGenericGBaseDialect()
            throws Exception {

        Set<String> fieldNames =
                Arrays.stream(
                                DatabaseIdentifier.class
                                        .getFields())
                        .map(Field::getName)
                        .collect(Collectors.toSet());

        assertFalse(
                "GBase family must keep 8c/8a/8s as separate dialect identities",
                fieldNames.contains("GBASE"));
    }
}
