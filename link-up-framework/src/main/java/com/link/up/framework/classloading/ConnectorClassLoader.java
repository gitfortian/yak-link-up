package com.link.up.framework.classloading;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

/**
 * Isolated connector loader: framework/API contracts come from the parent, connector libraries do not.
 */
public final class ConnectorClassLoader extends URLClassLoader {
    private static final String SOURCE_FACTORY_SERVICE =
            "META-INF/services/com.link.up.api.table.factory.TableSourceFactory";
    private static final String SINK_FACTORY_SERVICE =
            "META-INF/services/com.link.up.api.factory.SinkFactory";

    static {
        registerAsParallelCapable();
    }

    public ConnectorClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    private static boolean parentFirst(String name) {
        return name.startsWith("java.") || name.startsWith("javax.") || name.startsWith("jdk.")
                || name.startsWith("sun.") || name.startsWith("com.link.up.api.")
                || name.startsWith("org.slf4j.") || name.startsWith("org.apache.logging.");
    }

    private static boolean connectorFactoryService(String resourceName) {
        return SOURCE_FACTORY_SERVICE.equals(resourceName)
                || SINK_FACTORY_SERVICE.equals(resourceName);
    }

    /**
     * ServiceLoader normally inherits parent META-INF/services resources. For connector factory
     * discovery that would make every plugin directory rediscover built-in factories such as JDBC,
     * producing duplicate identifiers before the plugin's own factories can be registered.
     *
     * <p>Only Link-Up connector factory service descriptors are child-only. Other resources and
     * other ServiceLoader contracts retain normal URLClassLoader behavior.</p>
     */
    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        if (connectorFactoryService(name)) {
            return findResources(name);
        }
        return super.getResources(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loaded = findLoadedClass(name);
            if (loaded == null && parentFirst(name)) {
                try {
                    loaded = getParent().loadClass(name);
                } catch (ClassNotFoundException ignored) {
                }
            }
            if (loaded == null) {
                try {
                    loaded = findClass(name);
                } catch (ClassNotFoundException ignored) {
                }
            }
            if (loaded == null) loaded = getParent().loadClass(name);
            if (resolve) resolveClass(loaded);
            return loaded;
        }
    }
}
