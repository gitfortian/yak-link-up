package com.link.up.connector.file.internal;

import com.link.up.connector.file.config.FileSourceConfig;

/** Creates the storage implementation selected by the resolved config. */
public final class FileStorages {

    private FileStorages() {
    }

    public static FileStorage create(FileSourceConfig config) {
        if (config.isS3()) {
            return new S3FileStorage(config);
        }
        if (config.getStorageType() == FileSourceConfig.StorageType.SFTP) {
            return new SftpFileStorage(config);
        }
        return new LocalFileStorage();
    }
}
