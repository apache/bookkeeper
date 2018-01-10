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
package org.apache.distributedlog.statestore.impl.rocksdb.checkpoint;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.google.protobuf.UnsafeByteOperations;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.api.statestore.checkpoint.CheckpointManager;
import org.apache.distributedlog.api.statestore.exceptions.StateStoreException;
import org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils;
import org.apache.distributedlog.statestore.proto.CheckpointMetadata;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDBException;

/**
 * A task that periodically checkpoints rocksdb instance.
 */
@Slf4j
public class RocksdbCheckpointTask {

    private final String dbName;
    private final Checkpoint checkpoint;
    private final File checkpointDir;
    private final CheckpointManager checkpointManager;
    private final String dbPrefix;

    public RocksdbCheckpointTask(String dbName,
                                 Checkpoint checkpoint,
                                 File checkpointDir,
                                 CheckpointManager checkpointManager) {
        this.dbName = dbName;
        this.checkpoint = checkpoint;
        this.checkpointDir = checkpointDir;
        this.checkpointManager = checkpointManager;
        this.dbPrefix = String.format("%s", dbName);
    }

    public String checkpoint(byte[] txid) throws StateStoreException {
        String checkpointId = UUID.randomUUID().toString();

        File tempDir = new File(checkpointDir, checkpointId);
        try {
            try {
                checkpoint.createCheckpoint(tempDir.getAbsolutePath());
            } catch (RocksDBException e) {
                throw new StateStoreException("Failed to create a checkpoint at " + tempDir, e);
            }

            // get the files to copy
            List<File> filesToCopy = getFilesToCopy(tempDir);

            // copy the files
            copyFilesToDest(checkpointId, filesToCopy);

            // finalize copy files
            finalizeCopyFiles(checkpointId, filesToCopy);

            // dump the file list to checkpoint file
            finalizeCheckpoint(checkpointId, tempDir, txid);

            return checkpointId;
        } catch (IOException ioe) {
            log.error("Failed to checkpoint db {} to dir {}", new Object[] { dbName, tempDir, ioe });
            throw new StateStoreException(
                "Failed to checkpoint db " + dbName + " to dir " + tempDir,
                ioe);
        } finally {
            if (tempDir.exists()) {
                try {
                    MoreFiles.deleteRecursively(
                        Paths.get(tempDir.getAbsolutePath()),
                        RecursiveDeleteOption.ALLOW_INSECURE);
                } catch (IOException ioe) {
                    log.warn("Failed to remove temporary checkpoint dir {}", tempDir, ioe);
                }
            }
        }
    }

    private List<File> getFilesToCopy(File checkpointedDir) throws IOException {
        File[] files = checkpointedDir.listFiles();

        List<File> fileToCopy = Lists.newArrayListWithExpectedSize(files.length);
        for (File file : files) {
            if (RocksUtils.isSstFile(file)) {
                // sst files
                String destSstPath = RocksUtils.getDestSstPath(dbPrefix, file);
                // TODO: do more validation on the file
                if (!checkpointManager.fileExists(destSstPath)) {
                    fileToCopy.add(file);
                }
            } else {
                fileToCopy.add(file);
            }
        }
        return fileToCopy;
    }

    private void copyFilesToDest(String checkpointId, List<File> files) throws IOException {
        for (File file : files) {
            copyFileToDest(checkpointId, file);
        }

    }

    /**
     * All sst files are copied to checkpoint location first.
     */
    private void copyFileToDest(String checkpointId, File file) throws IOException {
        String destPath = RocksUtils.getDestPath(dbPrefix, checkpointId, file);
        try (OutputStream os = checkpointManager.openOutputStream(destPath)) {
            Files.copy(file, os);
        }
    }

    /**
     * Move the sst files to a common location.
     */
    private void finalizeCopyFiles(String checkpointId, List<File> files) throws IOException {
        for (File file : files) {
            if (RocksUtils.isSstFile(file)) {
                String destSstTempPath = RocksUtils.getDestTempSstPath(
                    dbPrefix, checkpointId, file);
                String destSstPath = RocksUtils.getDestSstPath(dbPrefix, file);
                checkpointManager.rename(destSstTempPath, destSstPath);
            }
        }
    }

    private void finalizeCheckpoint(String checkpointId,
                                    File checkpointedDir,
                                    byte[] txid) throws IOException {
        File[] files = checkpointedDir.listFiles();

        CheckpointMetadata.Builder metadataBuilder = CheckpointMetadata.newBuilder();
        for (File file : files) {
            metadataBuilder.addFiles(file.getName());
        }
        if (null != txid) {
            metadataBuilder.setTxid(UnsafeByteOperations.unsafeWrap(txid));
        }
        metadataBuilder.setCreatedAt(System.currentTimeMillis());

        String destCheckpointPath = RocksUtils.getDestCheckpointMetadataPath(dbPrefix, checkpointId);
        try (OutputStream os = checkpointManager.openOutputStream(destCheckpointPath)) {
            os.write(metadataBuilder.build().toByteArray());
        }
    }

}
