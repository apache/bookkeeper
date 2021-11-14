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

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreException;
import org.apache.bookkeeper.stream.proto.kv.store.CheckpointMetadata;


/**
 * CheckpointInfo encapsulated information and operatation for a checkpoint.
 */
@Slf4j
public class CheckpointInfo implements Comparable<CheckpointInfo> {

    final String id;
    CheckpointMetadata metadata;

    public CheckpointInfo(String id, InputStream is) throws IOException {
        this.id = id;
        this.metadata = CheckpointMetadata.parseFrom(is);
    }

    public CheckpointInfo(String id) {
        this.id = id;
    }
    public String getId() {
        return id;
    }

    public CheckpointMetadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "Checkpoint{ID='" + id + "', createdAt: " + getCreatedAt() + " " + metadata + "}";
    }

    // default fallback checkpoint. This essentially creates a blank store.
    public static CheckpointInfo nullCheckpoint() {
        return new CheckpointInfo(UUID.randomUUID().toString()) {
            public CheckpointMetadata restore(String dbName,
                                              File dbPath,
                                              CheckpointStore store,
                                              Duration maxIdle) throws StateStoreException {
                try {
                    Files.createDirectories(getCheckpointPath(dbPath));
                    updateCurrent(dbPath);
                } catch (IOException ioe) {
                    throw new StateStoreException("Failed to create dir " + dbName, ioe);
                }
                return null;
            }
            // for proper sorting
            public Long getCreatedAt() {
                return 0L;
            }
        };
    }

    public void remove(File dbPath) {
        try {
            Path checkpointPath = getCheckpointPath(dbPath);
            if (checkpointPath.toFile().exists()) {
                MoreFiles.deleteRecursively(checkpointPath, RecursiveDeleteOption.ALLOW_INSECURE);
            }
        } catch (IOException ioe) {
            log.warn("Failed to remove unused checkpoint {} from {}",
                id, getCheckpointBaseDir(dbPath), ioe);
        }
    }

    public Long getCreatedAt() {
        return this.metadata.getCreatedAt();
    }

    public File getCheckpointBaseDir(File dbPath) {
        return new File(dbPath, "checkpoints");
    }

    public Path getCheckpointPath(File dbPath) {
        return Paths.get(getCheckpointBaseDir(dbPath).getAbsolutePath(), id);
    }

    public File getCurrentDir(File dbPath) {
        return new File(dbPath, "current");
    }
    public Path getCurrentPath(File dbPath) {
        return Paths.get(getCurrentDir(dbPath).getAbsolutePath());
    }

    public void updateCurrent(File dbPath) throws IOException {
        Path currentPath = getCurrentPath(dbPath);
        Files.deleteIfExists(currentPath);
        Files.createSymbolicLink(currentPath, getCheckpointPath(dbPath));
    }

    @Override
    public int compareTo(CheckpointInfo o) {
        return this.getCreatedAt().compareTo(o.getCreatedAt());
    }

    public CheckpointMetadata restore(File dbPath, RocksdbRestoreTask task)
        throws StateStoreException, IOException, TimeoutException {

        task.restore(id, metadata);
        updateCurrent(dbPath);
        log.info("Successfully restore checkpoint {} to {}", id, getCheckpointPath(dbPath));
        return metadata;
    }
    public CheckpointMetadata restore(String dbName, File dbPath, CheckpointStore store)
        throws StateStoreException, TimeoutException {

        return restore(dbName, dbPath, store, Duration.ofMinutes(1));
    }

    public CheckpointMetadata restore(String dbName, File dbPath, CheckpointStore store, Duration maxIdle)
        throws StateStoreException, TimeoutException {

        try {
            File checkpointsDir = new File(dbPath, "checkpoints");
            RocksdbRestoreTask task = new RocksdbRestoreTask(dbName, checkpointsDir, store, maxIdle);
            return restore(dbPath, task);
        } catch (IOException ioe) {
            log.error("Failed to restore rocksdb {}", dbName, ioe);
            throw new StateStoreException("Failed to restore rocksdb " + dbName, ioe);
        }
    }
}
