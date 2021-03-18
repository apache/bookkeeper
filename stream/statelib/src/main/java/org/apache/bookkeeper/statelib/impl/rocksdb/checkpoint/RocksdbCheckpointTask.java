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

import com.google.common.collect.Sets;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.google.protobuf.UnsafeByteOperations;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreException;
import org.apache.bookkeeper.statelib.impl.rocksdb.RocksUtils;
import org.apache.bookkeeper.stream.proto.kv.store.CheckpointMetadata;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDBException;

/**
 * A task that periodically checkpoints rocksdb instance.
 */
@Slf4j
public class RocksdbCheckpointTask {


    /**
     * Error injection support for testing of the checkpoint.
     * @param <T>
     */
    @FunctionalInterface
    public interface InjectedError<T> {
        void accept(T t) throws IOException;
    }

    private final String dbName;
    private final Checkpoint checkpoint;
    private final File checkpointDir;
    private final CheckpointStore checkpointStore;
    private final String dbPrefix;
    private final boolean removeLocalCheckpointAfterSuccessfulCheckpoint;
    private final boolean removeRemoteCheckpointsAfterSuccessfulCheckpoint;
    private final boolean checkpointChecksumCompatible;
    private final boolean checkpointChecksumEnable;

    // for testing only
    private InjectedError<String> injectedError = (String checkpointId) -> {};

    public RocksdbCheckpointTask(String dbName,
                                 Checkpoint checkpoint,
                                 File checkpointDir,
                                 CheckpointStore checkpointStore,
                                 boolean removeLocalCheckpoint,
                                 boolean removeRemoteCheckpoints,
                                 boolean checkpointChecksumEnable,
                                 boolean checkpointChecksumCompatible) {
        this.dbName = dbName;
        this.checkpoint = checkpoint;
        this.checkpointDir = checkpointDir;
        this.checkpointStore = checkpointStore;
        this.dbPrefix = String.format("%s", dbName);
        this.removeLocalCheckpointAfterSuccessfulCheckpoint = removeLocalCheckpoint;
        this.removeRemoteCheckpointsAfterSuccessfulCheckpoint = removeRemoteCheckpoints;
        this.checkpointChecksumEnable = checkpointChecksumEnable;
        this.checkpointChecksumCompatible = checkpointChecksumCompatible;
    }

    public void setInjectedError(InjectedError<String> injectedError) {
        this.injectedError = injectedError;
    }

    public String checkpoint(byte[] txid) throws StateStoreException {
        String checkpointId = UUID.randomUUID().toString();

        File tempDir = new File(checkpointDir, checkpointId);
        log.info("Create a local checkpoint of state store {} at {}",
            dbName, tempDir);
        try {
            try {
                checkpoint.createCheckpoint(tempDir.getAbsolutePath());
            } catch (RocksDBException e) {
                throw new StateStoreException("Failed to create a checkpoint at " + tempDir, e);
            }

            String remoteCheckpointPath = RocksUtils.getDestCheckpointPath(dbPrefix, checkpointId);
            if (!checkpointStore.fileExists(remoteCheckpointPath)) {
                checkpointStore.createDirectories(remoteCheckpointPath);
            }
            String sstsPath = RocksUtils.getDestSstsPath(dbPrefix);
            if (!checkpointStore.fileExists(sstsPath)) {
                checkpointStore.createDirectories(sstsPath);
            }

            injectedError.accept(checkpointId);

            List<CheckpointFile> checkpointFiles = CheckpointFile.list(tempDir);
            List<CheckpointFile> filesToCopy = checkpointFiles.stream()
                .filter(f -> f.needCopy(checkpointStore, dbPrefix, checkpointChecksumEnable))
                .collect(Collectors.toList());

            // copy the files
            copyFilesToDest(checkpointId, filesToCopy);

            // finalize copy files
            finalizeCopyFiles(checkpointId, filesToCopy);

            // dump the file list to checkpoint file
            finalizeCheckpoint(checkpointFiles, checkpointId, txid);

            // clean up the remote checkpoints
            if (removeRemoteCheckpointsAfterSuccessfulCheckpoint) {
                cleanupRemoteCheckpoints(tempDir, checkpointId, checkpointFiles);
            }

            return checkpointId;
        } catch (IOException ioe) {
            log.error("Failed to checkpoint db {} to dir {}", new Object[] { dbName, tempDir, ioe });
            throw new StateStoreException(
                "Failed to checkpoint db " + dbName + " to dir " + tempDir,
                ioe);
        } finally {
            if (removeLocalCheckpointAfterSuccessfulCheckpoint && tempDir.exists()) {
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

    /**
     * All sst files are copied to checkpoint location first.
     */
    private void copyFilesToDest(String checkpointId, List<CheckpointFile> files) throws IOException {
        for (CheckpointFile file : files) {
            file.copyToRemote(checkpointStore, dbPrefix, checkpointId);
        }
    }

    /**
     * Move the sst files to a common location.
     */
    private void finalizeCopyFiles(String checkpointId,
                                   List<CheckpointFile> files) throws IOException {
        for (CheckpointFile file : files) {
            file.finalize(checkpointStore, dbPrefix, checkpointId,
                checkpointChecksumEnable, checkpointChecksumCompatible);
        }
    }

    private void finalizeCheckpoint(List<CheckpointFile> files,
                                    String checkpointId,
                                    byte[] txid) throws IOException {

        CheckpointMetadata.Builder metadataBuilder = CheckpointMetadata.newBuilder();
        for (CheckpointFile file : files) {
            if (checkpointChecksumEnable) {
                metadataBuilder.addFileInfos(file.getFileInfo());
            }
            metadataBuilder.addFiles(file.getName());
        }
        if (null != txid) {
            metadataBuilder.setTxid(UnsafeByteOperations.unsafeWrap(txid));
        }
        metadataBuilder.setCreatedAt(System.currentTimeMillis());

        String destCheckpointPath = RocksUtils.getDestCheckpointMetadataPath(dbPrefix, checkpointId);
        try (OutputStream os = checkpointStore.openOutputStream(destCheckpointPath)) {
            os.write(metadataBuilder.build().toByteArray());
        }
    }

    /**
     * Cleanup.
     *
     * <p>1) remove unneeded checkpoints
     * 2) remove unreferenced sst files.
     */
    private void cleanupRemoteCheckpoints(File checkpointedDir,
                                          String checkpointToExclude,
                                          List<CheckpointFile> filesToKeep) throws IOException {
        String checkpointsPath = RocksUtils.getDestCheckpointsPath(dbPrefix);
        List<String> checkpoints = checkpointStore.listFiles(checkpointsPath);

        // delete checkpoints
        for (String checkpoint : checkpoints) {
            if (checkpoint.equals(checkpointToExclude)) {
                continue;
            }
            String remoteCheckpointPath = RocksUtils.getDestCheckpointPath(dbPrefix, checkpoint);
            checkpointStore.deleteRecursively(
                remoteCheckpointPath);
            log.info("Delete remote checkpoint {} from checkpoint store at {}",
                checkpoint, remoteCheckpointPath);
        }

        // delete unused ssts
        Set<String> sstsToKeep = filesToKeep.stream()
            .filter(f -> f.isSstFile())
            .map(f -> f.getNameWithChecksum())
            .collect(Collectors.toSet());
        if (checkpointChecksumCompatible) {
            // If we are running in compatible mode, we need to retain sst files without checksum suffix.
            Set<String> files = filesToKeep.stream()
                .filter(f -> f.isSstFile())
                .map(f -> f.getName())
                .collect(Collectors.toSet());
            sstsToKeep.addAll(files);
        }
        Set<String> allSsts = checkpointStore.listFiles(RocksUtils.getDestSstsPath(dbPrefix))
            .stream()
            .collect(Collectors.toSet());

        Set<String> toDelete = Sets.difference(allSsts, sstsToKeep);
        for (String sst: toDelete) {
            checkpointStore.delete(RocksUtils.getDestSstPath(dbPrefix, sst));
        }
    }

}
