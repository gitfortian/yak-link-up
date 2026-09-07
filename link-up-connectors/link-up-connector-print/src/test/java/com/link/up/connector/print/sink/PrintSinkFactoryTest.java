package com.link.up.connector.print.sink;

import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.sink.PreparedSinkMetadata;
import com.link.up.api.sink.SinkPrepareContext;
import com.link.up.api.sink.SinkPreparer;
import com.link.up.api.sink.SinkWriter;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TablePath;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxRow;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrintSinkFactoryTest {

    private final PrintSinkFactory factory = new PrintSinkFactory();

    @Test
    public void shouldExposeIdentifierPrint() {
        assertEquals("print", factory.factoryIdentifier());
    }

    @Test
    public void shouldExposeEmptyCapabilities() {
        assertTrue(factory.capabilities().isEmpty());
    }

    @Test
    public void shouldDeclareNoRequiredOptions() {
        assertTrue(factory.optionRule().getRequiredOptions().isEmpty());
    }

    @Test
    public void shouldCreateWriterFromConfig() {
        SinkWriter<FluxRow> writer = factory.createSink(
                ReadonlyConfig.fromMap(new LinkedHashMap<String, Object>()),
                metadata());

        assertTrue(writer instanceof PrintSinkWriter);
    }

    @Test
    public void shouldUseDefaultNoopPreparer() throws Exception {
        SinkPreparer preparer = factory.createPreparer(
                ReadonlyConfig.fromMap(new LinkedHashMap<String, Object>()));

        Map<TablePath, CatalogTable> sourceTables =
                Collections.singletonMap(TablePath.of("datagen_table"), table());
        PreparedSinkMetadata metadata = preparer.prepare(
                new SinkPrepareContext(ReadonlyConfig.fromMap(new LinkedHashMap<String, Object>()), sourceTables));

        assertEquals(sourceTables, metadata.getTargetTables());
    }

    private static CatalogTable table() {
        return CatalogTable.builder(
                        TablePath.of("datagen_table"),
                        TableSchema.builder()
                                .column(Column.builder("id", BasicType.LONG_TYPE).build())
                                .build())
                .build();
    }

    private static PreparedSinkMetadata metadata() {
        return new PreparedSinkMetadata(
                Collections.singletonMap(TablePath.of("datagen_table"), table()));
    }
}
