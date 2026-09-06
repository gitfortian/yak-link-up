package com.link.up.framework.classloading;

import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConnectorClassLoaderTest {

    private static final String SOURCE_FACTORY_SERVICE =
            "META-INF/services/com.link.up.api.table.factory.TableSourceFactory";

    @Test
    public void connectorFactoryServiceDescriptorsAreChildOnly() throws Exception {
        Path parentRoot = Files.createTempDirectory("link-up-parent-services");
        Path childRoot = Files.createTempDirectory("link-up-child-services");
        writeService(parentRoot, SOURCE_FACTORY_SERVICE, "parent.Factory");
        writeService(childRoot, SOURCE_FACTORY_SERVICE, "child.Factory");

        URLClassLoader parent = new URLClassLoader(
                new URL[]{parentRoot.toUri().toURL()},
                getClass().getClassLoader());
        ConnectorClassLoader child = new ConnectorClassLoader(
                new URL[]{childRoot.toUri().toURL()},
                parent);
        try {
            List<URL> resources = list(child.getResources(SOURCE_FACTORY_SERVICE));
            assertEquals(1, resources.size());
            assertTrue(resources.get(0).toString().contains(childRoot.getFileName().toString()));
        } finally {
            child.close();
            parent.close();
        }
    }

    @Test
    public void ordinaryResourcesStillUseParentAndChildLookup() throws Exception {
        Path parentRoot = Files.createTempDirectory("link-up-parent-resource");
        Path childRoot = Files.createTempDirectory("link-up-child-resource");
        String resourceName = "connector-test-resource.txt";
        Files.write(parentRoot.resolve(resourceName), "parent".getBytes(StandardCharsets.UTF_8));
        Files.write(childRoot.resolve(resourceName), "child".getBytes(StandardCharsets.UTF_8));

        URLClassLoader parent = new URLClassLoader(
                new URL[]{parentRoot.toUri().toURL()},
                getClass().getClassLoader());
        ConnectorClassLoader child = new ConnectorClassLoader(
                new URL[]{childRoot.toUri().toURL()},
                parent);
        try {
            List<URL> resources = list(child.getResources(resourceName));
            assertEquals(2, resources.size());
        } finally {
            child.close();
            parent.close();
        }
    }

    private void writeService(Path root, String resourceName, String provider) throws Exception {
        Path file = root.resolve(resourceName);
        Files.createDirectories(file.getParent());
        Files.write(file, (provider + "\n").getBytes(StandardCharsets.UTF_8));
    }

    private List<URL> list(Enumeration<URL> resources) {
        List<URL> result = new ArrayList<URL>();
        while (resources.hasMoreElements()) {
            result.add(resources.nextElement());
        }
        return result;
    }
}
