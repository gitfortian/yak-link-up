package com.link.up.connector.file.source;

import com.google.auto.service.AutoService;
import com.link.up.api.configuration.ReadonlyConfig;
import com.link.up.api.configuration.util.OptionRule;
import com.link.up.api.connector.schema.ConnectorCapability;
import com.link.up.api.source.Source;
import com.link.up.api.source.SourceFactoryContext;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.factory.TableSourceFactory;
import com.link.up.connector.file.config.FileFormat;
import com.link.up.connector.file.config.FileSourceConfig;
import com.link.up.connector.file.config.FileSourceOptions;
import com.link.up.connector.file.converter.DelimitedRowConverter;
import com.link.up.connector.file.internal.FileEntry;
import com.link.up.connector.file.internal.FileStorage;
import com.link.up.connector.file.internal.FileStorages;
import com.link.up.connector.file.schema.FileSchemaResolver;
import org.apache.commons.csv.CSVFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * SPI factory for the bounded File Source.
 *
 * <p>Header discovery reads the first row of the first matched file. For
 * local storage that is a local read; for S3 it needs network access, so the
 * engine must only invoke {@link #discoverTableSchemas} on the explain path,
 * which its validate/explain contract already guarantees.
 */
@AutoService(TableSourceFactory.class)
public final class FileSourceFactory
        implements TableSourceFactory<FileSourceSplit> {

    @Override
    public String factoryIdentifier() {
        return "file";
    }

    @Override
    public Set<ConnectorCapability> capabilities() {
        return Collections.unmodifiableSet(
                EnumSet.of(
                        ConnectorCapability.TABLE_SCHEMA_DISCOVERY,
                        ConnectorCapability.PARTITION_SPLIT));
    }

    @Override
    public Source<FileSourceSplit> createSource(SourceFactoryContext context) {
        return new FileSource(createConfig(context));
    }

    @Override
    public List<CatalogTable> discoverTableSchemas(SourceFactoryContext context) throws Exception {
        FileSourceConfig config = createConfig(context);
        if (config.hasDeclaredSchema()
                || config.getFormat() == FileFormat.TEXT) {
            return Collections.singletonList(
                    FileSchemaResolver.toCatalogTable(
                            config,
                            FileSchemaResolver.fromDeclared(config.getDeclaredColumns())));
        }

        try (FileStorage storage = FileStorages.create(config)) {
            List<String> header = readFirstHeaderRow(config, storage);
            return Collections.singletonList(
                    FileSchemaResolver.toCatalogTable(
                            config,
                            FileSchemaResolver.fromHeader(header)));
        }
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(FileSourceOptions.PATH)
                .optional(
                        FileSourceOptions.STORAGE_TYPE,
                        FileSourceOptions.FORMAT,
                        FileSourceOptions.FIELDS,
                        FileSourceOptions.DELIMITER,
                        FileSourceOptions.QUOTE_CHAR,
                        FileSourceOptions.ESCAPE_CHAR,
                        FileSourceOptions.SKIP_HEADER_ROWS,
                        FileSourceOptions.NULL_VALUE,
                        FileSourceOptions.ENCODING,
                        FileSourceOptions.COMPRESSION,
                        FileSourceOptions.TABLE_NAME,
                        FileSourceOptions.SPLIT_SIZE,
                        FileSourceOptions.RECURSIVE,
                        FileSourceOptions.FILE_PATTERN,
                        FileSourceOptions.ENDPOINT,
                        FileSourceOptions.BUCKET,
                        FileSourceOptions.ACCESS_KEY,
                        FileSourceOptions.SECRET_KEY,
                        FileSourceOptions.REGION,
                        FileSourceOptions.PATH_STYLE_ACCESS)
                .exclusive(FileSourceOptions.SCHEMA, FileSourceOptions.HEADER)
                .build();
    }

    private List<String> readFirstHeaderRow(FileSourceConfig config, FileStorage storage)
            throws IOException {

        List<FileEntry> files = storage.listFiles(config.getPath(), config.isRecursive());
        for (FileEntry file : files) {
            if (file.getSize() == 0) {
                continue;
            }
            return parseHeaderLine(config, storage, file.getFileKey(), file.getSize());
        }
        throw new IllegalStateException(
                "No non-empty files under '" + config.getPath() + "' to discover a header row");
    }

    private List<String> parseHeaderLine(
            FileSourceConfig config,
            FileStorage storage,
            String fileKey,
            long size)
            throws IOException {

        InputStream range = storage.openRange(fileKey, 0L, Math.min(size, 1048576L));
        InputStream input = FileFormat.isGzipByName(fileNameOf(fileKey))
                ? new GZIPInputStream(range)
                : range;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, config.getEncoding()))) {

            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.isEmpty()) {
                throw new IllegalStateException(
                        "Cannot discover a header row from an empty first line of " + fileKey);
            }

            CSVFormat format = config.getFormat() == FileFormat.TSV
                    ? DelimitedRowConverter.tsvFormat()
                    : DelimitedRowConverter.csvFormat(config.getQuoteChar(), config.getEscapeChar());
            List<String> names = DelimitedRowConverter.parseSingleRecord(format, headerLine);
            List<String> trimmed = new ArrayList<String>(names.size());
            for (String name : names) {
                trimmed.add(name == null ? "" : name.trim());
            }
            return trimmed;
        }
    }

    private static String fileNameOf(String fileKey) {
        int nameStart = Math.max(fileKey.lastIndexOf('/'), fileKey.lastIndexOf('\\'));
        return nameStart < 0 ? fileKey : fileKey.substring(nameStart + 1);
    }

    private static FileSourceConfig createConfig(SourceFactoryContext context) {
        Objects.requireNonNull(context, "context must not be null");
        ReadonlyConfig options = Objects.requireNonNull(
                context.getOptions(),
                "source options must not be null");
        return FileSourceConfig.of(options);
    }
}
