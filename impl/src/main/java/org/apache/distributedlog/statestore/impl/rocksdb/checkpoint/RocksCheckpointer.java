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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.api.statestore.checkpoint.CheckpointManager;
import org.apache.distributedlog.api.statestore.exceptions.StateStoreException;
import org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils;
import org.apache.distributedlog.statestore.proto.CheckpointMetadata;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;

/**
 * Rocksdb Checkpointer that manages checkpoints.
 */
@Slf4j
public class RocksCheckpointer implements AutoCloseable {

    public static CheckpointMetadata restore(String dbName,
                                             File dbPath,
                                             CheckpointManager checkpointManager)
            throws StateStoreException {
        try {
            String dbPrefix = String.format("%s", dbName);

            Pair<String, CheckpointMetadata> latestCheckpoint = getLatestCheckpoint(
                dbPrefix, checkpointManager);
            File checkpointsDir = new File(dbPath, "checkpoints");
            String checkpointId = latestCheckpoint.getLeft();
            CheckpointMetadata checkpointMetadata = latestCheckpoint.getRight();
            if (checkpointId != null) {
                RocksdbRestoreTask task = new RocksdbRestoreTask(
                    dbName,
                    checkpointsDir,
                    checkpointManager);
                task.restore(checkpointId, checkpointMetadata);
            } else {
                // no checkpoints available, create an empty directory
                checkpointId = UUID.randomUUID().toString();
                new File(checkpointsDir, checkpointId).mkdir();
            }
            File currentDir = new File(dbPath, "current");
            if (currentDir.exists()) {
                currentDir.delete();
            }
            Files.createSymbolicLink(
                Paths.get(currentDir.getAbsolutePath()),
                Paths.get(dbPath.getAbsolutePath(), "checkpoints", checkpointId));
            return checkpointMetadata;
        } catch (IOException ioe) {
            log.error("Failed to restore rocksdb {}", dbName, ioe);
            throw new StateStoreException("Failed to restore rocksdb " + dbName, ioe);
        }
    }

    private static Pair<String, CheckpointMetadata > getLatestCheckpoint(
            String dbPrefix, CheckpointManager checkpointManager) throws IOException {
        List<String> files = checkpointManager.listFiles(dbPrefix);
        CheckpointMetadata latestCheckpoint = null;
        String latestCheckpointId = null;

        for (String checkpointId : files) {
            if ("ssts".equals(checkpointId)) {
                continue;
            }

            String metadataPath = RocksUtils.getDestCheckpointMetadataPath(
                dbPrefix,
                checkpointId);

            try (InputStream is = checkpointManager.openInputStream(metadataPath)) {
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
    private final CheckpointManager checkpointManager;

    public RocksCheckpointer(String dbName,
                             File dbPath,
                             RocksDB rocksDB,
                             CheckpointManager checkpointManager) {
        this.dbName = dbName;
        this.dbPath = dbPath;
        this.checkpoint = Checkpoint.create(rocksDB);
        this.checkpointManager = checkpointManager;
    }

    void checkpointAtTxid(byte[] txid) throws StateStoreException {

        RocksdbCheckpointTask task = new RocksdbCheckpointTask(
            dbName,
            checkpoint,
            new File(dbPath, "checkpoints"),
            checkpointManager);
        task.checkpoint(txid);

    }


    @Override
    public void close() {
        // no-op
    }
}
