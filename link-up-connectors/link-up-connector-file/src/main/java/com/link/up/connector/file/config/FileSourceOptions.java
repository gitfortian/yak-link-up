package com.link.up.connector.file.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.List;
import java.util.Map;

/** User-facing options for the bounded File Source. */
public final class FileSourceOptions {

    private FileSourceOptions() {
    }

    public static final Option<String> PATH =
            Options.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Local directory/file path, or s3://bucket/prefix/")
                    .withSemanticType("FILE_PATH")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> STORAGE_TYPE =
            Options.key("storage_type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("local or s3; inferred from the path scheme when absent")
                    .withSemanticType("STORAGE_TYPE")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("csv, tsv, text or jsonl; inferred from the file extension when absent")
                    .withSemanticType("FILE_FORMAT")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<List<Map>> SCHEMA =
            Options.key("schema")
                    .listType(Map.class)
                    .noDefaultValue()
                    .withDescription("Column definitions as {name, type}; text yields one content column")
                    .withSemanticType("SOURCE_SCHEMA")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<List<String>> FIELDS =
            Options.key("fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Optional column projection applied at the converter boundary")
                    .withSemanticType("SOURCE_FIELDS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> DELIMITER =
            Options.key("delimiter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Field delimiter for text format; csv stays RFC4180, tsv stays tab")
                    .withSemanticType("DELIMITER")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> QUOTE_CHAR =
            Options.key("quote_char")
                    .stringType()
                    .defaultValue("\"")
                    .withDescription("CSV field quote character")
                    .withSemanticType("QUOTE_CHAR")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> ESCAPE_CHAR =
            Options.key("escape_char")
                    .stringType()
                    .defaultValue("\"")
                    .withDescription("CSV in-field escape character")
                    .withSemanticType("ESCAPE_CHAR")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Long> SKIP_HEADER_ROWS =
            Options.key("skip_header_rows")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Leading lines to skip in every file, csv/tsv/text only")
                    .withSemanticType("SKIP_ROWS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Boolean> HEADER =
            Options.key("header")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription("First row is the header and provides column names; exclusive with schema")
                    .withSemanticType("HEADER_ROW")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> NULL_VALUE =
            Options.key("null_value")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Text token that converts to null, e.g. \\N")
                    .withSemanticType("NULL_VALUE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> ENCODING =
            Options.key("encoding")
                    .stringType()
                    .defaultValue("UTF-8")
                    .withDescription("File character encoding; no auto detection")
                    .withSemanticType("ENCODING")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> COMPRESSION =
            Options.key("compression")
                    .stringType()
                    .defaultValue("auto")
                    .withDescription("none, gz or auto (detected from the .gz extension)")
                    .withSemanticType("COMPRESSION")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Logical table name used as the data set id; derived from the path when absent")
                    .withSemanticType("TABLE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Long> SPLIT_SIZE =
            Options.key("split_size")
                    .longType()
                    .defaultValue(134217728L)
                    .withDescription("Target split size in bytes; minimum 1048576; gz files stay whole")
                    .withSemanticType("SPLIT_SIZE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<Boolean> RECURSIVE =
            Options.key("recursive")
                    .booleanType()
                    .defaultValue(Boolean.TRUE)
                    .withDescription("Recurse into sub directories / prefixes")
                    .withSemanticType("RECURSIVE")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<String> FILE_PATTERN =
            Options.key("file_pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("File name regex filter applied during enumeration")
                    .withSemanticType("FILE_FILTER")
                    .withScope(ConnectorOptionScope.RUNTIME);

    public static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3-compatible endpoint, e.g. MinIO/OSS/COS")
                    .withSemanticType("S3_ENDPOINT")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> BUCKET =
            Options.key("bucket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 bucket; may be carried by an s3:// path instead")
                    .withSemanticType("S3_BUCKET")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .sensitive()
                    .withDescription("S3 access key; falls back to the SDK default credential chain when absent")
                    .withSemanticType("ACCESS_KEY")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .sensitive()
                    .withDescription("S3 secret key; falls back to the SDK default credential chain when absent")
                    .withSemanticType("SECRET_KEY")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 region; required for AWS endpoints, optional for custom endpoints")
                    .withSemanticType("S3_REGION")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<Boolean> PATH_STYLE_ACCESS =
            Options.key("path_style_access")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription("Use path-style addressing, required by MinIO and similar stores")
                    .withSemanticType("S3_PATH_STYLE")
                    .withScope(ConnectorOptionScope.DATASOURCE);
}
