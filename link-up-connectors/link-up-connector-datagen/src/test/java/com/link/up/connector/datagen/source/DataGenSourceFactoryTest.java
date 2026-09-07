package com.link.up.connector.datagen.source;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.SqlType;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataGenSourceFactoryTest {

    private final DataGenSourceFactory factory = new DataGenSourceFactory();

    @Test
    public void shouldExposeIdentifierDatagen() {
        assertEquals("datagen", factory.factoryIdentifier());
    }

    @Test
    public void shouldExposeEmptyCapabilities() {
        assertTrue(factory.capabilities().isEmpty());
    }

    @Test
    public void shouldDeclareSchemaRequired() {
        boolean schemaRequired = false;
        for (com.link.up.api.configuration.util.RequiredOption group
                : factory.optionRule().getRequiredOptions()) {
            for (com.link.up.api.configuration.Option<?> option : group.getOptions()) {
                if ("schema".equals(option.key())) {
                    schemaRequired = true;
                }
            }
        }
        assertTrue("schema must be declared as a required option", schemaRequired);
    }

    @Test
    public void shouldDiscoverSchemaWithoutExternalAccess() throws Exception {
        List<CatalogTable> tables = factory.discoverTableSchemas(context());

        assertEquals(1, tables.size());
        CatalogTable table = tables.get(0);
        assertEquals("datagen_table", table.getTablePath().getTableName());

        TableSchema schema = table.getTableSchema();
        assertEquals(2, schema.getColumnCount());
        assertEquals(SqlType.BIGINT, schema.getColumn("id").getDataType().getSqlType());
        assertEquals(SqlType.DECIMAL, schema.getColumn("price").getDataType().getSqlType());
        assertEquals(Integer.valueOf(10), schema.getColumn("price").getPrecision());
        assertEquals(Integer.valueOf(2), schema.getColumn("price").getScale());
    }

    @Test
    public void shouldCreateSourceOfMatchingSplitType() throws Exception {
        Source<?> source = factory.createSource(context());

        assertTrue(source instanceof DataGenSource);
    }

    private static SourceFactoryContext context() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("schema", Arrays.asList(
                column("id", "bigint"),
                column("price", "decimal(10,2)")));
        return new SourceFactoryContext(ReadonlyConfig.fromMap(values));
    }

    private static Map<String, Object> column(String name, String type, Object... extraPairs) {
        Map<String, Object> column = new LinkedHashMap<String, Object>();
        column.put("name", name);
        column.put("type", type);
        for (int i = 0; i < extraPairs.length; i += 2) {
            column.put(String.valueOf(extraPairs[i]), extraPairs[i + 1]);
        }
        return column;
    }
}
