package com.link.up.connector.doris;

import com.link.up.connector.doris.converter.DorisArrowRowReader;
import com.link.up.connector.doris.converter.DorisRowSerializer;
import com.link.up.connector.doris.sink.DorisSinkWriter;
import com.link.up.connector.doris.source.DorisSourceFactory;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DorisConnectorPackageBoundaryTest {

    @Test
    public void rowSerializationBelongsToConverterRole() {
        assertEquals(
                "com.link.up.connector.doris.converter",
                DorisRowSerializer.class.getPackage().getName());
    }

    @Test
    public void nativeArrowDecodingBelongsToConverterRole() {
        assertEquals(
                "com.link.up.connector.doris.converter",
                DorisArrowRowReader.class.getPackage().getName());
        assertEquals(
                "com.link.up.connector.doris.source",
                DorisSourceFactory.class.getPackage().getName());
    }

    @Test
    public void sinkWriterDelegatesTwoPhaseCommitState() {
        boolean found = false;

        for (Field field : DorisSinkWriter.class.getDeclaredFields()) {
            if ("com.link.up.connector.doris.sink.DorisTwoPhaseCommitController"
                    .equals(field.getType().getName())) {
                found = true;
            }
        }

        assertTrue(
                "DorisSinkWriter should delegate 2PC state to a focused role",
                found);
    }

    @Test
    public void nativeSourceDoesNotIntroduceJdbcDataReaderRole() {
        File sourceRoot = new File("src/main/java/com/link/up/connector/doris/source");
        File[] files = sourceRoot.listFiles();
        assertTrue("Doris source package should exist", files != null && files.length > 0);
        for (File file : files) {
            assertFalse(
                    "Doris native Source must not add JDBC data readers: " + file.getName(),
                    file.getName().toLowerCase().contains("jdbc"));
        }
    }

    @Test
    public void forbiddenGenericRootPackagesMustNotExist() {
        File root = new File("src/main/java/com/link/up/connector/doris");

        String[] forbidden = {
                "common",
                "core",
                "helper",
                "misc",
                "parser",
                "serializer",
                "utils"
        };

        for (String name : forbidden) {
            assertFalse(
                    "Forbidden Doris connector root package exists: " + name,
                    new File(root, name).exists());
        }
    }
}
