package com.link.up.connector.file.converter;

import com.link.up.api.table.catalog.TableSchema;
import com.link.up.connector.file.config.FileFormat;
import com.link.up.connector.file.config.FileSourceConfig;
import com.link.up.connector.file.schema.FileSchemaResolver;

import java.util.List;
import java.util.Objects;

/** Creates the row converter selected by the resolved format. */
public final class FileRowConverters {

    private FileRowConverters() {
    }

    public static FileRowConverter create(
            FileSourceConfig config,
            TableSchema schema) {

        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(schema, "schema must not be null");

        List<String> projected = FileSchemaResolver.projectedNames(schema, config.getFields());
        List<Integer> outputIndexes = DelimitedRowConverter.outputIndexes(schema, projected);

        switch (config.getFormat()) {
            case CSV:
                return DelimitedRowConverter.csv(
                        schema,
                        outputIndexes,
                        config.getNullValue(),
                        config.getQuoteChar(),
                        config.getEscapeChar());
            case TSV:
                return DelimitedRowConverter.tsv(schema, outputIndexes, config.getNullValue());
            case JSONL:
                return new JsonLineRowConverter(schema, outputIndexes);
            case TEXT:
                return new TextRowConverter();
            default:
                throw new IllegalStateException("Unsupported file format: " + config.getFormat());
        }
    }
}
