package com.link.up.connector.http.catalog;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.table.catalog.Catalog;
import com.link.up.api.table.factory.CatalogFactory;

/**
 * HTTP Catalog SPI 工厂。
 *
 * <p>通过 {@link CatalogFactory} SPI 机制注册，
 * 控制面可按 {@code factoryIdentifier = "http"} 加载。
 */
@AutoService(CatalogFactory.class)
public final class HttpCatalogFactory
        implements CatalogFactory {

    private static final String IDENTIFIER = "http";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog createCatalog(
            String catalogName,
            ReadonlyConfig options) {

        HttpCatalogConfig config =
                HttpCatalogConfig.of(options);

        return new HttpCatalog(
                catalogName,
                config);
    }
}
