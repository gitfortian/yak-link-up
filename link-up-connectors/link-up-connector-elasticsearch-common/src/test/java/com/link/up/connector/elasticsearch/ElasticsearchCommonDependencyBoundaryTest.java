package com.link.up.connector.elasticsearch;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticsearchCommonDependencyBoundaryTest {

    @Test
    public void commonModuleMustNotSeeVersionedElasticsearchSdk() {
        assertClassNotPresent("org.elasticsearch.client.RestHighLevelClient");
        assertClassNotPresent("co.elastic.clients.elasticsearch.ElasticsearchClient");
    }

    @Test
    public void connectorIdentifiersAreStableAndDistinct() {
        assertTrue(ElasticsearchConnectorFamily.ELASTICSEARCH7_IDENTIFIER.startsWith(
                ElasticsearchConnectorFamily.FAMILY_IDENTIFIER));
        assertTrue(ElasticsearchConnectorFamily.ELASTICSEARCH8_IDENTIFIER.startsWith(
                ElasticsearchConnectorFamily.FAMILY_IDENTIFIER));
        assertNotEquals(
                ElasticsearchConnectorFamily.ELASTICSEARCH7_IDENTIFIER,
                ElasticsearchConnectorFamily.ELASTICSEARCH8_IDENTIFIER);
    }

    @Test
    public void versionedConnectorsMustNotBeFlattenedIntoCurrentRuntimeClasspath()
            throws IOException {
        String launcherPom = readRepositoryFile("link-up-launcher/pom.xml");
        String serverPom = readRepositoryFile("link-up-server/pom.xml");

        assertNotFlattened(launcherPom, "link-up-launcher");
        assertNotFlattened(serverPom, "link-up-server");
    }

    private static void assertNotFlattened(String pom, String moduleName) {
        assertFalse(
                moduleName + " must not directly depend on Elasticsearch 7 before runtime isolation exists",
                pom.contains("<artifactId>link-up-connector-elasticsearch7</artifactId>"));
        assertFalse(
                moduleName + " must not directly depend on Elasticsearch 8 before runtime isolation exists",
                pom.contains("<artifactId>link-up-connector-elasticsearch8</artifactId>"));
    }

    private static String readRepositoryFile(String relativePath) throws IOException {
        Path file = findRepositoryRoot().resolve(relativePath);
        if (!Files.isRegularFile(file)) {
            fail("Expected repository file does not exist: " + file);
        }
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static Path findRepositoryRoot() {
        Path current = Paths.get("").toAbsolutePath();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("pom.xml"))
                    && Files.isDirectory(current.resolve("link-up-connectors"))
                    && Files.isDirectory(current.resolve("link-up-launcher"))) {
                return current;
            }
            current = current.getParent();
        }
        fail("Could not locate Link-Up repository root from current working directory");
        return Paths.get("");
    }

    private static void assertClassNotPresent(String className) {
        try {
            Class.forName(className);
            fail("Version-specific Elasticsearch SDK leaked into common module: " + className);
        } catch (ClassNotFoundException expected) {
            // Expected: common deliberately has no Elasticsearch SDK dependency.
        }
    }
}
