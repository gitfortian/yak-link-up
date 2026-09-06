package com.link.up.server;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Guards the runtime packaging boundary required to load Elasticsearch 7 and 8 together. */
public class ElasticsearchPluginPackagingBoundaryTest {

    @Test
    public void distributionKeepsVersionedConnectorsOutOfFlatRuntimeScope() throws Exception {
        String pom = read(repositoryRoot().resolve("link-up-dist/pom.xml"));

        assertProvidedDependency(pom, "link-up-connector-elasticsearch7");
        assertProvidedDependency(pom, "link-up-connector-elasticsearch8");
    }

    @Test
    public void distributionUsesOneDirectoryPerElasticsearchClassLoader() throws Exception {
        String assembly = read(repositoryRoot().resolve(
                "link-up-dist/src/main/assembly/distribution.xml"));

        assertTrue(assembly.contains("<outputDirectory>plugins/elasticsearch7</outputDirectory>"));
        assertTrue(assembly.contains("<outputDirectory>plugins/elasticsearch8</outputDirectory>"));
        assertTrue(assembly.contains(
                "link-up-connector-elasticsearch7/target/plugin-dependencies"));
        assertTrue(assembly.contains(
                "link-up-connector-elasticsearch8/target/plugin-dependencies"));
        assertFalse(assembly.contains("<outputDirectory>plugins/elasticsearch</outputDirectory>"));
    }

    @Test
    public void eachVersionResolvesItsRuntimeGraphInsideItsOwnModule() throws Exception {
        String es7 = read(repositoryRoot().resolve(
                "link-up-connectors/link-up-connector-elasticsearch7/pom.xml"));
        String es8 = read(repositoryRoot().resolve(
                "link-up-connectors/link-up-connector-elasticsearch8/pom.xml"));

        assertTrue(es7.contains("stage-elasticsearch7-plugin-dependencies"));
        assertTrue(es8.contains("stage-elasticsearch8-plugin-dependencies"));
        assertTrue(es7.contains("${project.build.directory}/plugin-dependencies"));
        assertTrue(es8.contains("${project.build.directory}/plugin-dependencies"));
    }

    @Test
    public void distributionLauncherPassesSubdirectoriesAsSeparatePluginDirs() throws Exception {
        String script = read(repositoryRoot().resolve(
                "link-up-dist/src/main/dist/bin/link-up-server.sh"));

        assertTrue(script.contains("LINK_UP_PLUGIN_ROOT"));
        assertTrue(script.contains("LINK_UP_PLUGIN_DIRS"));
        assertTrue(script.contains("PLUGIN_ARGS+=( --plugin-dir"));
        assertTrue(script.contains("\"${PLUGIN_ROOT}\"/*"));
        assertFalse(script.contains("-cp \"${LINK_UP_HOME}/lib/*:${LINK_UP_HOME}/plugins"));
    }

    private void assertProvidedDependency(String pom, String artifactId) {
        String marker = "<artifactId>" + artifactId + "</artifactId>";
        int start = pom.indexOf(marker);
        assertTrue("missing dependency " + artifactId, start >= 0);

        int end = pom.indexOf("</dependency>", start);
        assertTrue("unterminated dependency " + artifactId, end > start);
        String dependency = pom.substring(start, end);
        assertTrue(
                artifactId + " must remain reactor-only/provided",
                dependency.contains("<scope>provided</scope>"));
    }

    private Path repositoryRoot() {
        Path current = Paths.get("").toAbsolutePath().normalize();
        if (Files.exists(current.resolve("link-up-dist/pom.xml"))) {
            return current;
        }
        Path parent = current.getParent();
        if (parent != null && Files.exists(parent.resolve("link-up-dist/pom.xml"))) {
            return parent;
        }
        throw new IllegalStateException("Could not locate Link-Up repository root from " + current);
    }

    private String read(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
