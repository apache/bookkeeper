/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint;

import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;
import org.apache.bookkeeper.statelib.impl.rocksdb.RocksUtils;
import org.apache.bookkeeper.stream.proto.kv.store.CheckpointMetadata;
import org.apache.bookkeeper.stream.proto.kv.store.FileInfo;

/**
 * CheckpointFile encapsulates the attributes and operations for a file in checkpoint.
 */
@Slf4j
@lombok.Builder
@lombok.EqualsAndHashCode
public class CheckpointFile {
    private final File file;
    private final String checksum;
    private final boolean isSstFile;

    private CheckpointFile(File file, String checksum, boolean isSstFile) {
        this.file = file;
        this.checksum = checksum;
        this.isSstFile = isSstFile;
    }

    /**
     * CheckpointFileBuilder for building instances of CheckpointFile objects.
     */
    public static class CheckpointFileBuilder {
        CheckpointFileBuilder file(File file) {
            this.file = file;
            this.isSstFile = RocksUtils.isSstFile(this.file);
            return this;
        }
        CheckpointFileBuilder file(File checkpointDir, String filename) {
            file(new File(checkpointDir, filename));
            return this;
        }

        CheckpointFileBuilder computeChecksum() {
            return checksum(computeChecksum(this.file));
        }

        private static String computeChecksum(File file) {
            String ckSum = "invalid-" + System.currentTimeMillis();
            try {
                ckSum = Files.asByteSource(file).hash(Hashing.sha256()).toString();
                return ckSum;
            } catch (IOException e) {
                log.error("Failed to get checksum for file {} {}", file.getName(), e.getMessage(), e);
            }
            return ckSum;
        }
    }

    public File getFile() {
        return file;
    }

    public String toString() {
        return String.format("CheckpointFile: %s", file.getName());
    }

    public boolean isSstFile() {
        return isSstFile;
    }

    public String getName() {
        return file.getName();
    }

    public String getNameWithChecksum() {
        if (checksum != null) {
            return file.getName() + "_" + checksum;
        }
        return getName();
    }

    public String getRemoteSstPath(String dbPrefix, boolean enableChecksum) {
        if (enableChecksum) {
            return RocksUtils.getDestSstPath(dbPrefix, getNameWithChecksum());
        } else {
            return RocksUtils.getDestSstPath(dbPrefix, getName());
        }
    }

    public String getRemotePath(String dbPrefix, String checkpointId, boolean enableChecksum) {
        if (isSstFile) {
            return getRemoteSstPath(dbPrefix, enableChecksum);
        }
        return RocksUtils.getDestPath(dbPrefix, checkpointId, file);
    }

    public static List<CheckpointFile> list(File checkpointedDir) {
        // List for files from checkpoint folder
        return Arrays.stream(checkpointedDir.listFiles())
            .map(f -> CheckpointFile.builder()
                .file(f)
                .computeChecksum()
                .build())
            .collect(Collectors.toList());
    }

    public static List<CheckpointFile> list(File checkpointDir, CheckpointMetadata metadata) {
        // List for files from checkpoint metadata
        if (metadata.getFileInfosCount() != 0) {
            return metadata.getFileInfosList().stream()
                .map(f -> CheckpointFile.builder()
                    .file(checkpointDir, f.getName())
                    .checksum(f.getChecksum())
                    .build())
                .collect(Collectors.toList());
        } else {
            // Old checkpoint without checksums
            return metadata.getFilesList().stream()
                .map(f -> CheckpointFile.builder()
                    .file(checkpointDir, f)
                    .build())
                .collect(Collectors.toList());
        }
    }

    public boolean needCopy(CheckpointStore checkpointStore, String dbPrefix, boolean enableChecksum) {
        if (!isSstFile) {
            // Always copy the non SST files
            return true;
        }
        // sst files
        String destSstPath = getRemoteSstPath(dbPrefix, enableChecksum);
        try {
            if (checkpointStore.fileExists(destSstPath)) {
                return false;
            }
        } catch (IOException e) {
            log.error("Failed fileExists {} {}", file.getName(), e.getMessage(), e);
        }
        return true;
    }

    public void copyToRemote(CheckpointStore checkpointStore, String dbPrefix, String checkpointId) throws IOException {
        String destPath = RocksUtils.getDestPath(dbPrefix, checkpointId, getName());
        try (OutputStream os = checkpointStore.openOutputStream(destPath)) {
            Files.copy(file, os);
        }
    }

    public void finalize(CheckpointStore checkpointStore,
                         String dbPrefix,
                         String checkpointId,
                         boolean enableChecksum,
                         boolean enableNonChecksumCompatibility) throws IOException {
        if (!RocksUtils.isSstFile(file)) {
            return;
        }
        // Move the SST file to common area where it can be shared among other checkpoints
        String destSstTempPath = RocksUtils.getDestPath(dbPrefix, checkpointId, getName());
        String destSstPath = getRemoteSstPath(dbPrefix, enableChecksum);
        checkpointStore.rename(destSstTempPath, destSstPath);

        if (enableChecksum && enableNonChecksumCompatibility) {
            copyToRemoteWithoutChecksum(checkpointStore, dbPrefix, checkpointId);
        }
    }

    public void copyToRemoteWithoutChecksum(CheckpointStore checkpointStore,
                                            String dbPrefix,
                                            String checkpointId) throws IOException {
        String destPath = getRemoteSstPath(dbPrefix, false);

        try (OutputStream os = checkpointStore.openOutputStream(destPath)) {
            Files.copy(file, os);
        }
    }

    public void copyFromRemote(CheckpointStore checkpointStore,
                               String dbPrefix,
                               String checkpointId) throws IOException {
        String remoteFilePath = getRemotePath(dbPrefix, checkpointId, true);

        try (InputStream is = checkpointStore.openInputStream(remoteFilePath)) {
            java.nio.file.Files.copy(
                is,
                Paths.get(file.getAbsolutePath()),
                StandardCopyOption.REPLACE_EXISTING);
        }
    }


    public FileInfo getFileInfo() {
        return FileInfo.newBuilder()
            .setName(file.getName())
            .setChecksum(checksum)
            .build();
    }
}
