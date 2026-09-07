package com.link.up.connector.file.config;

import com.link.up.api.configuration.ReadonlyConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileSourceConfigTest {

    @Test
    public void shouldInferStorageFromPathScheme() {
        FileSourceConfig config = FileSourceConfig.of(ReadonlyConfig.fromMap(
                baseS3()));

        assertTrue(config.isS3());
        assertEquals("bucket", config.getBucket());
        assertEquals("prefix/", config.getPath());
        assertEquals("us-east-1", config.getRegion());
    }

    @Test
    public void shouldFailWhenS3BucketMissing() {
        Map<String, Object> values = baseS3();
        values.put("path", "s3:///");

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected a missing S3 bucket to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("bucket"));
        }
    }

    @Test
    public void shouldFailWhenExplicitStorageConflictsWithScheme() {
        Map<String, Object> values = baseLocalCsv();
        values.put("storage_type", "s3");

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected conflicting storage_type to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("conflicts"));
        }
    }

    @Test
    public void shouldInferFormatFromExtension() {
        FileSourceConfig config = FileSourceConfig.of(ReadonlyConfig.fromMap(baseLocalCsv()));

        assertFalse(config.isS3());
        assertEquals(FileFormat.CSV, config.getFormat());
        assertEquals("rows", config.getTableName());
        assertFalse(config.isGzipFile("rows.csv"));
    }

    @Test
    public void shouldRequireFormatWhenExtensionUnknown() {
        Map<String, Object> values = baseLocalCsv();
        values.put("path", "some/dir/without/extension");

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected format to be required without a known extension");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("format"));
        }
    }

    @Test
    public void shouldRejectSchemaWhenHeaderEnabled() {
        Map<String, Object> values = baseLocalCsv();
        values.put("header", true);

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected schema and header together to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("mutually exclusive"));
        }
    }

    @Test
    public void shouldRejectExplicitNoneCompressionForGzExtension() {
        Map<String, Object> values = baseLocalCsv();
        values.put("path", "data/rows.csv.gz");
        values.put("compression", "none");

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected compression=none with a .gz path to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("conflicts"));
        }
    }

    @Test
    public void shouldAutoDetectGzCompression() {
        Map<String, Object> values = baseLocalCsv();
        values.put("path", "data/rows.csv.gz");

        FileSourceConfig config = FileSourceConfig.of(ReadonlyConfig.fromMap(values));

        assertTrue(config.isGzipFile("rows.csv.gz"));
        assertFalse(config.isGzipFile("rows.csv"));
        assertEquals("rows", config.getTableName());
    }

    @Test
    public void shouldRejectSplitSizeBelowMinimum() {
        Map<String, Object> values = baseLocalCsv();
        values.put("split_size", 1024L);

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected split_size below the minimum to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("split_size"));
        }
    }

    @Test
    public void shouldRejectRegionMissingForAwsEndpoint() {
        Map<String, Object> values = baseS3();
        values.remove("region");

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected a missing region for AWS endpoints to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("region"));
        }
    }

    @Test
    public void shouldRejectAccessKeyWithoutSecretKey() {
        Map<String, Object> values = baseS3();
        values.put("access_key", "ak");

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected access_key without secret_key to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("together"));
        }
    }

    @Test
    public void shouldRejectDelimiterForCsv() {
        Map<String, Object> values = baseLocalCsv();
        values.put("delimiter", "|");

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected a custom delimiter on csv to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("delimiter"));
        }
    }

    @Test
    public void shouldRejectUnknownEncoding() {
        Map<String, Object> values = baseLocalCsv();
        values.put("encoding", "NOT_A_CHARSET");

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected an unknown encoding to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("encoding"));
        }
    }

    @Test
    public void shouldRejectSplitUnsafeEncoding() {
        Map<String, Object> values = baseLocalCsv();
        values.put("encoding", "UTF-16");

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected UTF-16 to be rejected for byte-level split alignment");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("ASCII-compatible"));
        }
    }

    @Test
    public void shouldResolveTextSchemaAsSingleContentColumn() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", "data/notes.txt");

        FileSourceConfig config = FileSourceConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals(1, config.getDeclaredColumns().size());
        assertEquals("content", config.getDeclaredColumns().get(0).getName());
    }

    @Test
    public void shouldInferSftpFromScheme() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", "sftp:///data/orders");
        values.put("format", "text");
        values.put("host", "10.0.0.1");
        values.put("user", "sync");
        values.put("password", "pass");

        FileSourceConfig config = FileSourceConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals(FileSourceConfig.StorageType.SFTP, config.getStorageType());
        assertEquals("/data/orders", config.getPath());
        assertEquals("orders", config.getTableName());
    }

    @Test
    public void shouldAcceptExplicitSftpWithPlainPath() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", "/data/orders");
        values.put("storage_type", "sftp");
        values.put("format", "text");
        values.put("host", "10.0.0.1");
        values.put("user", "sync");
        values.put("private_key", "/home/me/id_rsa");

        FileSourceConfig config = FileSourceConfig.of(ReadonlyConfig.fromMap(values));

        assertEquals(FileSourceConfig.StorageType.SFTP, config.getStorageType());
        assertEquals("/data/orders", config.getPath());
        assertEquals("orders", config.getTableName());
    }

    @Test
    public void shouldRequireHostAndUserForSftp() {
        Map<String, Object> values = baseSftpWithoutAuth();
        values.remove("host");
        values.put("password", "pass");

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected sftp without host to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("host"));
        }
    }

    @Test
    public void shouldRequireAuthForSftp() {
        Map<String, Object> values = baseSftpWithoutAuth();

        try {
            FileSourceConfig.of(ReadonlyConfig.fromMap(values));
            fail("Expected sftp without an authentication method to be rejected");
        } catch (IllegalArgumentException failure) {
            assertTrue(failure.getMessage().contains("password or private_key"));
        }
    }

    private static Map<String, Object> baseSftpWithoutAuth() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", "/data/orders");
        values.put("storage_type", "sftp");
        values.put("format", "text");
        values.put("host", "10.0.0.1");
        values.put("user", "sync");
        return values;
    }

    private static Map<String, Object> baseLocalCsv() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", "data/rows.csv");
        values.put("schema", schema());
        return values;
    }

    private static Map<String, Object> baseS3() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("path", "s3://bucket/prefix/");
        values.put("format", "csv");
        values.put("region", "us-east-1");
        values.put("schema", schema());
        return values;
    }

    private static List<Map<String, Object>> schema() {
        Map<String, Object> column = new LinkedHashMap<String, Object>();
        column.put("name", "id");
        column.put("type", "bigint");
        return Collections.singletonList(column);
    }
}
