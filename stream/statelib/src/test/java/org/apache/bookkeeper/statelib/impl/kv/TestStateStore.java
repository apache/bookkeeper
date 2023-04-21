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
package org.apache.bookkeeper.statelib.impl.kv;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreException;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.CheckpointInfo;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.RocksCheckpointer;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.RocksdbCheckpointTask;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.RocksdbRestoreTask;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.fs.FSCheckpointManager;
import org.apache.bookkeeper.stream.proto.kv.store.CheckpointMetadata;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.rocksdb.Checkpoint;


/**
 * TestStateStore is a helper class for testing various statestore operations.
 */
@Slf4j
public class TestStateStore {

    private final String dbName;
    private boolean removeLocal;
    private boolean removeRemote;

    private File localDir;
    private File localCheckpointsDir;
    private File remoteDir;
    private Path remoteCheckpointsPath;
    private StateStoreSpec spec;
    private RocksdbKVStore<String, String> store;
    private CheckpointStore checkpointStore;
    private RocksdbCheckpointTask checkpointer;
    private RocksdbRestoreTask restorer;
    private ScheduledExecutorService checkpointExecutor;
    private boolean checkpointChecksumEnable;
    private boolean checkpointChecksumCompatible;
    private boolean enableNonChecksumCompatibility;
    private boolean localStorageCleanup;
    private Duration checkpointRestoreIdleWait;

    public TestStateStore(String dbName,
                          File localDir,
                          File remoteDir,
                          boolean removeLocal,
                          boolean removeRemote) throws IOException {
        this.dbName = dbName;
        this.localDir = localDir;
        this.remoteDir = remoteDir;
        this.removeLocal = removeLocal;
        this.removeRemote = removeRemote;
        this.checkpointChecksumEnable = true;
        this.checkpointChecksumCompatible = true;
        this.localStorageCleanup = false;
        localCheckpointsDir = new File(localDir, "checkpoints");
        remoteCheckpointsPath = Paths.get(remoteDir.getAbsolutePath(), dbName);
        enableNonChecksumCompatibility = false;
    }
    public TestStateStore(TestName runtime, TemporaryFolder testDir) throws IOException {
        this(
            runtime.getMethodName(),
            testDir.newFolder("local"),
            testDir.newFolder("remote"),
            false,
            false
        );
    }

    public void checkpointChecksumEnable(boolean val) {
        checkpointChecksumEnable = val;
    }

    public void checkpointChecksumCompatible(boolean val) {
        checkpointChecksumCompatible = val;
    }

    public File getLocalDir() {
        return localDir;
    }

    public File getRemoteDir() {
        return remoteDir;
    }

    public void enableCheckpoints(boolean enable) {
        if (enable) {
            checkpointExecutor = Executors.newSingleThreadScheduledExecutor();
        } else {
            checkpointExecutor.shutdown();
            checkpointExecutor = null;
        }
    }

    public void setRemoveLocal(boolean enable) {
        removeLocal = enable;
    }

    public void setRemoveRemote(boolean enable) {
        removeRemote = enable;
    }

    public void setLocalStorageCleanup(boolean enable) {
        localStorageCleanup = enable;
    }

    public File getLocalCheckpointsDir() {
        return localCheckpointsDir;
    }

    public CheckpointStore getCheckpointStore() {
        return checkpointStore;
    }

    public void setCheckpointRestoreIdleWait(Duration d) {
        checkpointRestoreIdleWait = d;
    }

    public CheckpointStore newCheckpointStore() {
        return new FSCheckpointManager(remoteDir);
    }

    public void init() throws StateStoreException {
        checkpointStore = newCheckpointStore();
        StateStoreSpec.StateStoreSpecBuilder builder = StateStoreSpec.builder()
            .name(dbName)
            .keyCoder(StringUtf8Coder.of())
            .valCoder(StringUtf8Coder.of())
            .localStateStoreDir(localDir)
            .checkpointChecksumEnable(checkpointChecksumEnable)
            .checkpointChecksumCompatible(checkpointChecksumCompatible)
            .localStorageCleanupEnable(localStorageCleanup)
            .stream(dbName);
        if (checkpointExecutor != null) {
            builder = builder.checkpointStore(checkpointStore)
                .checkpointIOScheduler(checkpointExecutor);
        }
        if (checkpointRestoreIdleWait != null) {
            builder = builder.checkpointRestoreIdleLimit(checkpointRestoreIdleWait);
        }
        spec = builder.build();
        store = new RocksdbKVStore<>();
        store.init(spec);

        this.checkpointer = new RocksdbCheckpointTask(
            dbName, Checkpoint.create(store.getDb()), localCheckpointsDir, checkpointStore,
            removeLocal, removeRemote, spec.isCheckpointChecksumEnable(),
            spec.isCheckpointChecksumCompatible());
        this.restorer = new RocksdbRestoreTask(dbName, localCheckpointsDir, checkpointStore);
    }

    public void close() {
        store.close();
    }

    public void destroyLocal() {
        store.close();

        try {
            // delete the checkpoints
            for (File f: localCheckpointsDir.listFiles()) {
                Path p = f.toPath();
                MoreFiles.deleteRecursively(f.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
            }
            // remove `current` symlink
            new File(localDir, "current").delete();
        } catch (Exception e) {
            // ignore
        }
    }

    public String checkpoint(String checkpointID) throws StateStoreException {
        byte[] txid = checkpointID.getBytes();
        return checkpointer.checkpoint(txid);
    }

    List<CheckpointInfo> getCheckpoints() {
        return RocksCheckpointer.getCheckpoints(store.name(), checkpointStore);
    }
    public CheckpointInfo getLatestCheckpoint() {
        List<CheckpointInfo> checkpoints = RocksCheckpointer.getCheckpoints(store.name(), checkpointStore);
        return checkpoints.get(0);
    }

    public void restore() throws Exception {
        store.close();
        if (checkpointExecutor != null) {
            checkpointExecutor.submit(() -> {}).get();
        }
        this.init();
    }

    CheckpointMetadata restore(CheckpointInfo checkpoint) throws StateStoreException, TimeoutException {
        try {
            MoreFiles.deleteRecursively(
                checkpoint.getCheckpointPath(localDir),
                RecursiveDeleteOption.ALLOW_INSECURE);
        } catch (Exception e) {
            // ignore
        }

        CheckpointMetadata md = checkpoint.restore(store.name(), localDir, checkpointStore);
        store.close();
        store = new RocksdbKVStore<>();
        store.init(spec);
        this.init();
        return md;
    }


    private static String getKey(int i) {
        return String.format("key-%06d", i);
    }

    private static String getValue(int i) {
        return String.format("val-%06d", i);
    }

    public void addNumKVs(String txId, int numKvs, int startIdx) throws StateStoreException {
        for (int i = 0; i < numKvs; i++) {
            String key = getKey(startIdx + i);
            String val = getValue(startIdx + i);

            store.put(key, val);
        }
        store.put("transaction-id", txId);
        store.flush();
    }

    public void addNumKVs(int numKvs, int startIdx) throws StateStoreException {
        for (int i = 0; i < numKvs; i++) {
            String key = getKey(startIdx + i);
            String val = getValue(startIdx + i);

            store.put(key, val);
        }
        store.flush();
    }

    public String get(String key) {
        return store.get(key);
    }

    public void corruptCheckpoint(CheckpointInfo cpi) throws IOException {
        File checkpointDir = cpi.getCheckpointPath(remoteCheckpointsPath.toFile()).toFile();

        File current = new File(checkpointDir, "CURRENT");
        FileWriter w = new FileWriter(current);
        w.write("MANIFEST-xxxx\n");
        w.close();
    }
}
