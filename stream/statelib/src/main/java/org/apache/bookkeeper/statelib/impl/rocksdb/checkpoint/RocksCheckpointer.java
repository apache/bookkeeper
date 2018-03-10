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

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreException;
import org.apache.bookkeeper.statelib.impl.rocksdb.RocksUtils;
import org.apache.bookkeeper.stream.proto.kv.store.CheckpointMetadata;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;

/**
 * Rocksdb Checkpointer that manages checkpoints.
 */
@Slf4j
public class RocksCheckpointer implements AutoCloseable {

    public static CheckpointMetadata restore(String dbName,
                                             File dbPath,
                                             CheckpointStore checkpointStore)
        throws StateStoreException {
        try {
            String dbPrefix = String.format("%s", dbName);

            Pair<String, CheckpointMetadata> latestCheckpoint = getLatestCheckpoint(
                dbPrefix, checkpointStore);
            File checkpointsDir = new File(dbPath, "checkpoints");
            String checkpointId = latestCheckpoint.getLeft();
            CheckpointMetadata checkpointMetadata = latestCheckpoint.getRight();
            if (checkpointId != null) {
                RocksdbRestoreTask task = new RocksdbRestoreTask(
                    dbName,
                    checkpointsDir,
                    checkpointStore);
                task.restore(checkpointId, checkpointMetadata);
            } else {
                // no checkpoints available, create an empty directory
                checkpointId = UUID.randomUUID().toString();
                Files.createDirectories(
                    Paths.get(checkpointsDir.getAbsolutePath(), checkpointId));
            }
            Path restoredCheckpointPath = Paths.get(checkpointsDir.getAbsolutePath(), checkpointId);
            log.info("Successfully restore checkpoint {} to {}", checkpointId, restoredCheckpointPath);

            File currentDir = new File(dbPath, "current");
            Files.deleteIfExists(Paths.get(currentDir.getAbsolutePath()));
            Files.createSymbolicLink(
                Paths.get(currentDir.getAbsolutePath()),
                restoredCheckpointPath);

            // after successfully restore from remote checkpoints, cleanup other unused checkpoints
            cleanupLocalCheckpoints(checkpointsDir, checkpointId);

            return checkpointMetadata;
        } catch (IOException ioe) {
            log.error("Failed to restore rocksdb {}", dbName, ioe);
            throw new StateStoreException("Failed to restore rocksdb " + dbName, ioe);
        }
    }

    private static void cleanupLocalCheckpoints(File checkpointsDir, String checkpointToExclude) {
        String[] checkpoints = checkpointsDir.list();
        for (String checkpoint : checkpoints) {
            if (checkpoint.equals(checkpointToExclude)) {
                continue;
            }
            try {
                MoreFiles.deleteRecursively(
                    Paths.get(checkpointsDir.getAbsolutePath(), checkpoint),
                    RecursiveDeleteOption.ALLOW_INSECURE);
            } catch (IOException ioe) {
                log.warn("Failed to remove unused checkpoint {} from {}",
                    checkpoint, checkpointsDir, ioe);
            }
        }
    }

    private static Pair<String, CheckpointMetadata> getLatestCheckpoint(
        String dbPrefix, CheckpointStore checkpointStore) throws IOException {
        String remoteCheckpointsPath = RocksUtils.getDestCheckpointsPath(dbPrefix);
        List<String> files = checkpointStore.listFiles(remoteCheckpointsPath);
        CheckpointMetadata latestCheckpoint = null;
        String latestCheckpointId = null;

        for (String checkpointId : files) {
            String metadataPath = RocksUtils.getDestCheckpointMetadataPath(
                dbPrefix,
                checkpointId);

            try (InputStream is = checkpointStore.openInputStream(metadataPath)) {
                CheckpointMetadata ckpt = CheckpointMetadata.parseFrom(is);
                if (null == latestCheckpoint) {
                    latestCheckpointId = checkpointId;
                    latestCheckpoint = ckpt;
                } else if (latestCheckpoint.getCreatedAt() < ckpt.getCreatedAt()) {
                    latestCheckpointId = checkpointId;
                    latestCheckpoint = ckpt;
                }
            }
        }
        return Pair.of(latestCheckpointId, latestCheckpoint);
    }

    private final String dbName;
    private final File dbPath;
    private final Checkpoint checkpoint;
    private final CheckpointStore checkpointStore;
    private final boolean removeLocalCheckpointAfterSuccessfulCheckpoint;
    private final boolean removeRemoteCheckpointsAfterSuccessfulCheckpoint;

    public RocksCheckpointer(String dbName,
                             File dbPath,
                             RocksDB rocksDB,
                             CheckpointStore checkpointStore,
                             boolean removeLocalCheckpointAfterSuccessfulCheckpoint,
                             boolean removeRemoteCheckpointsAfterSuccessfulCheckpoint) {
        this.dbName = dbName;
        this.dbPath = dbPath;
        this.checkpoint = Checkpoint.create(rocksDB);
        this.checkpointStore = checkpointStore;
        this.removeLocalCheckpointAfterSuccessfulCheckpoint = removeLocalCheckpointAfterSuccessfulCheckpoint;
        this.removeRemoteCheckpointsAfterSuccessfulCheckpoint = removeRemoteCheckpointsAfterSuccessfulCheckpoint;
    }

    public String checkpointAtTxid(byte[] txid) throws StateStoreException {
        RocksdbCheckpointTask task = new RocksdbCheckpointTask(
            dbName,
            checkpoint,
            new File(dbPath, "checkpoints"),
            checkpointStore,
            removeLocalCheckpointAfterSuccessfulCheckpoint,
            removeRemoteCheckpointsAfterSuccessfulCheckpoint);
        return task.checkpoint(txid);
    }


    @Override
    public void close() {
        // no-op
    }
}
