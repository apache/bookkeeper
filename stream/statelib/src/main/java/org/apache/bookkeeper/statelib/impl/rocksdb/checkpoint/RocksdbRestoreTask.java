/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreException;
import org.apache.bookkeeper.statelib.impl.rocksdb.RocksUtils;
import org.apache.bookkeeper.stream.proto.kv.store.CheckpointMetadata;

/**
 * A task that restore a rocksdb instance.
 */
@Slf4j
public class RocksdbRestoreTask {

    private final String dbName;
    private final File checkpointDir;
    private final CheckpointStore checkpointStore;
    private final String dbPrefix;

    public RocksdbRestoreTask(String dbName,
                              File checkpointDir,
                              CheckpointStore checkpointStore) {
        this.dbName = dbName;
        this.checkpointDir = checkpointDir;
        this.checkpointStore = checkpointStore;
        this.dbPrefix = String.format("%s", dbName);
    }

    public void restore(String checkpointId, CheckpointMetadata metadata) throws StateStoreException {
        File checkpointedDir = new File(checkpointDir, checkpointId);

        try {
            List<String> filesToCopy = getFilesToCopy(checkpointId, checkpointedDir, metadata);

            copyFilesFromRemote(checkpointId, checkpointedDir, filesToCopy);
        } catch (IOException ioe) {
            log.error("Failed to restore checkpoint {} to local directory {}",
                new Object[] { checkpointId, checkpointedDir, ioe });
            throw new StateStoreException(
                "Failed to restore checkpoint " + checkpointId + " to local directory " + checkpointedDir,
                ioe);
        }
    }

    private List<String> getFilesToCopy(String checkpointId,
                                        File checkpointedDir,
                                        CheckpointMetadata metadata) throws IOException {
        if (!checkpointedDir.exists()) {
            Files.createDirectories(
                Paths.get(checkpointedDir.getAbsolutePath()));
        }

        List<String> filesToCopy = Lists.newArrayListWithExpectedSize(metadata.getFilesCount());
        for (String fileName : metadata.getFilesList()) {
            File localFile = new File(checkpointedDir, fileName);
            if (!localFile.exists()) {
                filesToCopy.add(fileName);
                continue;
            }

            String srcFile;
            if (RocksUtils.isSstFile(localFile)) {
                srcFile = RocksUtils.getDestSstPath(dbPrefix, localFile);
            } else {
                srcFile = RocksUtils.getDestPath(dbPrefix, checkpointId, localFile);
            }

            long srcFileLength = checkpointStore.getFileLength(srcFile);
            long localFileLength = localFile.length();
            if (srcFileLength != localFileLength) {
                filesToCopy.add(fileName);
            }
        }

        return filesToCopy;
    }

    private void copyFilesFromRemote(String checkpointId,
                                     File checkpointedDir,
                                     List<String> remoteFiles) throws IOException {
        for (String file : remoteFiles) {
            copyFileFromRemote(checkpointId, checkpointedDir, file);
        }
    }

    private void copyFileFromRemote(String checkpointId,
                                    File checkpointedDir,
                                    String remoteFile) throws IOException {
        File localFile = new File(checkpointedDir, remoteFile);
        String remoteFilePath;
        if (RocksUtils.isSstFile(localFile)) {
            remoteFilePath = RocksUtils.getDestSstPath(dbPrefix, localFile);
        } else {
            remoteFilePath = RocksUtils.getDestPath(dbPrefix, checkpointId, localFile);
        }

        byte[] data = new byte[128 * 1024];
        try (InputStream is = checkpointStore.openInputStream(remoteFilePath)) {
            Files.copy(
                is,
                Paths.get(localFile.getAbsolutePath()),
                StandardCopyOption.REPLACE_EXISTING);
        }
    }

}
