/*
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
 */
package org.apache.bookkeeper.statelib.impl.kv;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.statelib.testing.executors.MockExecutorController.THREAD_NAME_PREFIX;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.coder.ByteArrayCoder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.api.exceptions.InvalidStateStoreException;
import org.apache.bookkeeper.statelib.impl.rocksdb.RocksUtils;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.fs.FSCheckpointManager;
import org.apache.bookkeeper.statelib.testing.executors.MockExecutorController;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Unit test of {@link RocksdbKVStore}.
 */
@Slf4j
public class TestRocksdbKVAsyncStoreWithCheckpoints extends TestDistributedLogBase {

    private static URI uri;
    private static Namespace namespace;

    @BeforeClass
    public static void setupCluster() throws Exception {
        TestDistributedLogBase.setupCluster();
        uri = DLMTestUtil.createDLMURI(zkPort, "/mvcc");
        conf.setPeriodicFlushFrequencyMilliSeconds(2);
        conf.setWriteLockEnabled(false);
        conf.setExplicitTruncationByApplication(true);
        namespace = NamespaceBuilder.newBuilder()
            .conf(conf)
            .uri(uri)
            .clientId("test-mvcc-async-store")
            .build();
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        if (null != namespace) {
            namespace.close();
        }
        TestDistributedLogBase.teardownCluster();
    }

    @Rule
    public final TestName runtime = new TestName();
    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();

    private String streamName;
    private RocksdbKVAsyncStore<byte[], byte[]> store;
    private ScheduledExecutorService mockWriteExecutor;
    private ExecutorService realWriteExecutor;
    private MockExecutorController writeExecutorController;
    private ScheduledExecutorService checkpointExecutor;
    private FSCheckpointManager checkpointStore;
    private File localDir;
    private File remoteDir;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        ensureURICreated(uri);

        localDir = testDir.newFolder("local");
        remoteDir = testDir.newFolder("remote");

        checkpointStore = new FSCheckpointManager(remoteDir);

        // initialize the scheduler
        realWriteExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat(THREAD_NAME_PREFIX + "%d").build());
        mockWriteExecutor = mock(ScheduledExecutorService.class);
        writeExecutorController = new MockExecutorController(realWriteExecutor)
            .controlExecute(mockWriteExecutor)
            .controlSubmit(mockWriteExecutor)
            .controlSchedule(mockWriteExecutor)
            .controlScheduleAtFixedRate(mockWriteExecutor, 5);

        checkpointExecutor = Executors.newSingleThreadScheduledExecutor();

        store = new RocksdbKVAsyncStore<>(
            () -> new RocksdbKVStore<>(),
            () -> namespace);
        FutureUtils.result(store.init(initSpec(runtime.getMethodName())));
    }

    private StateStoreSpec initSpec(String streamName) {
        return StateStoreSpec.builder()
            .name(streamName)
            .keyCoder(ByteArrayCoder.of())
            .valCoder(ByteArrayCoder.of())
            .stream(streamName)
            .localStateStoreDir(localDir)
            .writeIOScheduler(mockWriteExecutor)
            .checkpointStore(checkpointStore)
            .checkpointIOScheduler(checkpointExecutor)
            .checkpointDuration(Duration.ofMinutes(1))
            .checkpointChecksumEnable(true)
            .checkpointChecksumCompatible(false)
            .build();
    }

    @After
    @Override
    public void teardown() throws Exception {
        if (null != streamName) {
            namespace.deleteLog(streamName);
        }

        if (null != store) {
            store.close();
        }

        if (null != checkpointExecutor) {
            checkpointExecutor.shutdown();
        }

        if (null != realWriteExecutor) {
            realWriteExecutor.shutdown();
        }

        if (null != checkpointStore) {
            checkpointStore.close();
        }

        super.teardown();
    }

    @Test(expected = NullPointerException.class)
    public void testInitMissingStreamName() throws Exception {
        this.streamName = "test-init-missing-stream-name";
        StateStoreSpec spec = StateStoreSpec.builder()
            .name(streamName)
            .keyCoder(ByteArrayCoder.of())
            .valCoder(ByteArrayCoder.of())
            .localStateStoreDir(localDir)
            .build();
        result(store.init(spec));
    }

    private byte[] getKey(int i) {
        return String.format("key-%05d", i).getBytes(UTF_8);
    }

    private byte[] getValue(int i) {
        return String.format("value-%05d", i).getBytes(UTF_8);
    }

    private void writeNumKvs(int numKvs, int startIdx) throws Exception {
        CompletableFuture<Void> lastWrite = null;
        for (int i = 0; i < numKvs; i++) {
            byte[] key = getKey(startIdx + i);
            byte[] val = getValue(startIdx + i);

            lastWrite = store.put(key, val);
        }
        FutureUtils.result(lastWrite);
    }

    private void readNumKvs(int numKvs, int startIdx) throws Exception {
        for (int i = 0; i < numKvs; i++) {
            byte[] key = getKey(startIdx + i);

            byte[] expectedVal = getValue(startIdx + i);
            byte[] actualVal = FutureUtils.result(store.get(key));

            assertArrayEquals(expectedVal, actualVal);
        }
    }


    @Test
    public void testRestartNoCheckpoint() throws Exception {
        final int numKvs = 100;

        // write 100 kvs
        writeNumKvs(numKvs, 0);

        // close the store
        store.close();

        // since the lock isn't advanced so no checkpoint is created.
        List<String> files = checkpointStore.listFiles(RocksUtils.getDestCheckpointsPath(store.name()));
        assertTrue(files.isEmpty());

        // remove local dir
        MoreFiles.deleteRecursively(
            Paths.get(localDir.getAbsolutePath()),
            RecursiveDeleteOption.ALLOW_INSECURE);

        // reload the store
        store = new RocksdbKVAsyncStore<>(
            () -> new RocksdbKVStore<>(),
            () -> namespace);
        FutureUtils.result(store.init(initSpec(runtime.getMethodName())));

        assertTrue(Files.exists(
            Paths.get(localDir.getAbsolutePath())));

        // verify the 100 kvs
        readNumKvs(numKvs, 0);
    }

    @Test
    public void testRestartWithCheckpoint() throws Exception {
        final int numKvs = 100;

        // write 100 kvs
        writeNumKvs(numKvs, 0);

        // advance the clock to trigger checkpointing
        writeExecutorController.advance(Duration.ofMinutes(1));

        // ensure checkpoint completed
        checkpointExecutor.submit(() -> {
        }).get();

        store.close();

        List<String> files = checkpointStore.listFiles(RocksUtils.getDestCheckpointsPath(store.name()));
        assertEquals(1, files.size());

        // remove local dir
        MoreFiles.deleteRecursively(
            Paths.get(localDir.getAbsolutePath()),
            RecursiveDeleteOption.ALLOW_INSECURE);

        // reload the store
        store = new RocksdbKVAsyncStore<>(
            () -> new RocksdbKVStore<>(),
            () -> namespace);
        FutureUtils.result(store.init(initSpec(runtime.getMethodName())));

        assertTrue(Files.exists(
            Paths.get(localDir.getAbsolutePath())));

        // verify the 100 kvs
        readNumKvs(numKvs, 0);
    }

    private void reinitStore(StateStoreSpec spec) throws Exception {
        store.close();

        // remove local dir
        MoreFiles.deleteRecursively(
            Paths.get(localDir.getAbsolutePath()),
            RecursiveDeleteOption.ALLOW_INSECURE);

        store = new RocksdbKVAsyncStore<>(
            () -> new RocksdbKVStore<>(),
            () -> namespace);
        result(store.init(spec));
    }

    @Test
    public void testMissingData() throws Exception {
        int numKeys = 100;
        this.streamName = runtime.getMethodName();
        StateStoreSpec spec = initSpec(runtime.getMethodName());

        reinitStore(spec);

        writeNumKvs(numKeys, 0);
        readNumKvs(numKeys, 0);

        // Reload store from Journal
        reinitStore(spec);

        readNumKvs(numKeys, 0);

        // Trigger a checkpoint
        store.getLocalStore().checkpoint();
        // ensure checkpoint completed
        checkpointExecutor.submit(() -> {}).get();

        writeNumKvs(numKeys, 100);

        reinitStore(spec);

        // Validate that we can load from both checkpoint and journal
        readNumKvs(numKeys, 0);
        readNumKvs(numKeys, 100);

        // Add some more data
        writeNumKvs(numKeys, 200);
        DLSN dlsn = store.getLastDLSN();

        // Truncate and purge part of Journal
        result(store.truncateJournal(dlsn));
        store.purgeOlderThan(Integer.MAX_VALUE);

        // reloading the statestore should fail.
        try {
            reinitStore(spec);
            fail("Store initialization should fail due to missing data");
        } catch (InvalidStateStoreException isse) {
            assertEquals(
                "replayJournal failed: Invalid starting transaction 204 expecting 102 for stream testMissingData",
                isse.getMessage());
        }
    }
}
