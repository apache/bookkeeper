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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreException;
import org.apache.bookkeeper.statelib.impl.rocksdb.RocksUtils;
import org.apache.bookkeeper.stream.proto.kv.store.CheckpointMetadata;
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
        CheckpointInfo checkpoint = getLatestCheckpoint(dbName, checkpointStore);
        return restore(checkpoint, dbName, dbPath, checkpointStore);
    }

    public static CheckpointMetadata restore(CheckpointInfo checkpoint,
                                             String dbName,
                                             File dbPath,
                                             CheckpointStore checkpointStore)
        throws StateStoreException {
        checkpoint.restore(dbName, dbPath, checkpointStore);
        // after successfully restore from remote checkpoints, cleanup other unused checkpoints
        cleanupLocalCheckpoints(dbPath, checkpoint.getId());

        return checkpoint.getMetadata();
    }

    private static void cleanupLocalCheckpoints(File dbPath, String checkpointToExclude) {
        File checkpointsDir = new File(dbPath, "checkpoints");
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

    private static CheckpointInfo getLatestCheckpoint(
        String dbPrefix, CheckpointStore checkpointStore) {
        List<CheckpointInfo> checkpoints = getCheckpoints(dbPrefix, checkpointStore);
        if (checkpoints.size() <= 0) {
            // Even if there are not checkpoints available, there should be a
            // nullCheckpoint in the list
            throw new RuntimeException("Checkpoint list can't be empty");
        }
        return checkpoints.get(0);
    }

    public static List<CheckpointInfo> getCheckpoints(String dbPrefix, CheckpointStore store) {
        String remoteCheckpointsPath = RocksUtils.getDestCheckpointsPath(dbPrefix);
        ArrayList<CheckpointInfo> result = new ArrayList<>();
        result.add(CheckpointInfo.nullCheckpoint());
        List<String> files;
        try {
            files = store.listFiles(remoteCheckpointsPath);
        } catch (IOException e) {
            log.warn("No remote checkpoints available. Starting with nullCheckpoint", e);
            return result;
        }

        for (String checkpointId : files) {
            String metadataPath = RocksUtils.getDestCheckpointMetadataPath(
                dbPrefix,
                checkpointId);

            try (InputStream is = store.openInputStream(metadataPath)) {
                result.add(new CheckpointInfo(checkpointId, is));
            } catch (FileNotFoundException fnfe) {
                log.error("Metadata is corrupt for the checkpoint {}. Skipping it.", checkpointId);
            } catch (IOException e) {
                log.error("IO exception {}, Skipping it", checkpointId, e);
            }
        }
        Collections.sort(result, Collections.reverseOrder());
        return result;
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
