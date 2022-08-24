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
package org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.kv.KV;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreException;
import org.apache.bookkeeper.statelib.api.kv.KVIterator;
import org.apache.bookkeeper.statelib.impl.kv.RocksdbKVStore;
import org.apache.bookkeeper.statelib.impl.kv.TestStateStore;
import org.apache.bookkeeper.statelib.impl.rocksdb.RocksUtils;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.fs.FSCheckpointManager;
import org.apache.bookkeeper.stream.proto.kv.store.CheckpointMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.rocksdb.Checkpoint;

/**
 * Unit test of {@link RocksCheckpointer}.
 */
@Slf4j
public class RocksCheckpointerTest {

    @Rule
    public final TestName runtime = new TestName();
    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();

    private File localDir;
    private File localCheckpointsDir;
    private File remoteDir;
    private StateStoreSpec spec;
    private RocksdbKVStore<String, String> store;
    private CheckpointStore checkpointStore;

    @Before
    public void setUp() throws Exception {
        localDir = testDir.newFolder("local");
        localCheckpointsDir = new File(localDir, "checkpoints");
        assertTrue(
            "Not able to create checkpoints directory",
            localCheckpointsDir.mkdir());
        remoteDir = testDir.newFolder("remote");

        checkpointStore = new FSCheckpointManager(remoteDir);

        spec = StateStoreSpec.builder()
            .name(runtime.getMethodName())
            .keyCoder(StringUtf8Coder.of())
            .valCoder(StringUtf8Coder.of())
            .localStateStoreDir(localDir)
            .stream(runtime.getMethodName())
            .build();
        store = new RocksdbKVStore<>();
        store.init(spec);
    }

    @After
    public void tearDown() throws Exception {
        if (null != store) {
            store.close();
        }
        if (null != checkpointStore) {
            checkpointStore.close();
        }
    }

    private static String getKey(int i) {
        return String.format("key-%06d", i);
    }

    private static String getValue(int i) {
        return String.format("val-%06d", i);
    }

    private void writeNumKvs(int numKvs, int startIdx) throws Exception {
        for (int i = 0; i < numKvs; i++) {
            String key = getKey(startIdx + i);
            String val = getValue(startIdx + i);

            store.put(key, val);
        }
        store.flush();
    }

    private void verifyNumKvs(int expectedNumKvs) throws Exception {
        try (KVIterator<String, String> iter = store.range(null, null)) {
            int idx = 0;
            while (iter.hasNext()) {
                KV<String, String> kv = iter.next();
                assertEquals(getKey(idx), kv.key());
                assertEquals(getValue(idx), kv.value());
                ++idx;
            }
            assertEquals(expectedNumKvs, idx);
        }
    }

    private void verifyCheckpointMetadata(File checkpointedDir,
                                          CheckpointMetadata metadata) {
        List<CheckpointFile> files = CheckpointFile.list(checkpointedDir);
        assertNotNull(files);
        assertEquals(files.size(), metadata.getFileInfosCount());

        Set<CheckpointFile> localFiles = Sets.newHashSet(files);
        Set<CheckpointFile> metadataFiles = metadata.getFileInfosList()
            .stream()
            .map(f -> CheckpointFile.builder()
                .file(checkpointedDir, f.getName())
                .computeChecksum()
                .build())
            .collect(Collectors.toSet());

        assertEquals(localFiles, metadataFiles);
    }

    private void verifyRemoteFiles(String checkpointId, File checkpointedDir) throws Exception {
        List<CheckpointFile> files = CheckpointFile.list(checkpointedDir);
        assertNotNull(files);

        for (CheckpointFile file : files) {
            String remoteFile = file.getRemotePath(store.name(), checkpointId, true);
            verifyRemoteFile(remoteFile, file.getFile());
        }
    }

    private void verifyRemoteFile(String remoteFile, File localFile) throws Exception {
        assertEquals(checkpointStore.getFileLength(remoteFile), localFile.length());

        @Cleanup InputStream remoteIs = checkpointStore.openInputStream(remoteFile);
        @Cleanup InputStream localIs = new FileInputStream(localFile);

        long numBytesToCompare = localFile.length();

        while (numBytesToCompare > 0L) {
            int numBytesToRead = (int) Math.min(numBytesToCompare, 1024);
            byte[] localBytes = new byte[numBytesToRead];
            byte[] remoteBytes = new byte[numBytesToRead];

            ByteStreams.readFully(localIs, localBytes);
            ByteStreams.readFully(remoteIs, remoteBytes);
            assertArrayEquals(localBytes, remoteBytes);

            numBytesToCompare -= numBytesToRead;
        }
    }

    private List<String> createMultipleCheckpoints(int numCheckpoints,
                                                   boolean removeLocalCheckpointAfterCheckpoint,
                                                   boolean removeRemoteCheckpointsAfterCheckpoint)
            throws Exception {
        List<String> checkpointIds = new ArrayList<>(numCheckpoints);
        try (RocksCheckpointer checkpointer = new RocksCheckpointer(
            store.name(),
            localDir,
            store.getDb(),
            checkpointStore,
            removeLocalCheckpointAfterCheckpoint,
            removeRemoteCheckpointsAfterCheckpoint,
            true,
            false)) {
            int idx = 0;
            for (int i = 0; i < numCheckpoints; i++) {
                writeNumKvs(100, idx);
                checkpointIds.add(createCheckpoint(checkpointer, i));
                idx += 100;
            }
        }
        return checkpointIds;
    }

    private String createCheckpoint(RocksCheckpointer checkpointer,
                                    int checkpointIdx) throws StateStoreException {
        byte[] txid = ("checkpoint-" + checkpointIdx).getBytes(UTF_8);
        return checkpointer.checkpointAtTxid(txid);
    }

    /**
     * Basic test.
     *
     * <p>- checkpoint a local state store to a remote checkpoint store
     * - restore checkpoint from the remote checkpoint store
     * - verify the restored local state store is correct
     */
    @Test
    public void testCheckpointRestore() throws Exception {
        final int numKvs = 100;
        final String dbName = runtime.getMethodName();
        final byte[] txid = runtime.getMethodName().getBytes(UTF_8);

        // first prepare rocksdb with 100 kvs;
        writeNumKvs(numKvs, 0);

        Checkpoint checkpoint = Checkpoint.create(store.getDb());

        // checkpoint
        RocksdbCheckpointTask checkpointTask = new RocksdbCheckpointTask(
            dbName,
            checkpoint,
            localCheckpointsDir,
            checkpointStore,
            false,
            false,
            true,
            false);

        String checkpointId = checkpointTask.checkpoint(txid);
        // checkpoint directory exists
        File checkpointedDir = new File(localCheckpointsDir, checkpointId);
        assertTrue(
            "local checkpointed dir " + checkpointedDir + " doesn't exists when `removeLocalCheckpoints` is false",
            checkpointedDir.exists());

        // remote checkpoint metadata file exists
        String checkpointMetadataFile = RocksUtils.getDestCheckpointMetadataPath(store.name(), checkpointId);
        assertTrue(checkpointStore.fileExists(checkpointMetadataFile));
        int fileLen = (int) checkpointStore.getFileLength(checkpointMetadataFile);
        byte[] checkpointMetadataBytes = new byte[fileLen];
        @Cleanup InputStream fileIn = checkpointStore.openInputStream(checkpointMetadataFile);
        ByteStreams.readFully(fileIn, checkpointMetadataBytes);

        // verify the checkpointed metadata exists
        CheckpointMetadata metadata = CheckpointMetadata.parseFrom(checkpointMetadataBytes);
        assertArrayEquals(txid, metadata.getTxid().toByteArray());
        verifyCheckpointMetadata(checkpointedDir, metadata);
        verifyRemoteFiles(checkpointId, checkpointedDir);

        store.close();

        // remove local checkpointed dir
        MoreFiles.deleteRecursively(
            Paths.get(checkpointedDir.getAbsolutePath()),
            RecursiveDeleteOption.ALLOW_INSECURE);
        assertFalse(checkpointedDir.exists());

        // restore the checkpoint
        RocksdbRestoreTask restoreTask = new RocksdbRestoreTask(
            dbName,
            localCheckpointsDir,
            checkpointStore);
        restoreTask.restore(checkpointId, metadata);
        assertTrue(checkpointedDir.exists());

        // verify the content
        verifyCheckpointMetadata(checkpointedDir, metadata);
        verifyRemoteFiles(checkpointId, checkpointedDir);

        // make sure all the kvs are readable
        store = new RocksdbKVStore<>();
        store.init(spec);

        verifyNumKvs(numKvs);
    }

    @Test
    public void testRestoreCleanupCheckpoints() throws Exception {
        // create 3 checkpoints and leave them locally
        List<String> checkpointIds = createMultipleCheckpoints(3, false, false);
        store.close();

        List<String> remoteCheckpoints = checkpointStore.listFiles(RocksUtils.getDestCheckpointsPath(store.name()));
        assertEquals(checkpointIds.size(), remoteCheckpoints.size());

        for (String checkpoint : checkpointIds) {
            assertTrue(remoteCheckpoints.contains(checkpoint));
            assertTrue(new File(localCheckpointsDir, checkpoint).exists());
        }

        // restore from checkpoints
        CheckpointMetadata metadata = RocksCheckpointer.restore(
            store.name(),
            localDir,
            checkpointStore);
        assertNotNull(metadata);
        assertArrayEquals("checkpoint-2".getBytes(UTF_8), metadata.getTxid().toByteArray());

        for (int i = 0; i < 3; i++) {
            String checkpoint = checkpointIds.get(i);
            if (i == 2) {
                assertTrue(new File(localCheckpointsDir, checkpoint).exists());
            } else {
                assertFalse(new File(localCheckpointsDir, checkpoint).exists());
            }
            assertTrue(
                checkpointStore.fileExists(RocksUtils.getDestCheckpointPath(store.name(), checkpoint)));
        }

        // restore from the latest checkpoint
        store = new RocksdbKVStore<>();
        store.init(spec);

        verifyNumKvs(300);
    }
    @Test
    public void testCheckpointOrder() throws Exception {
        List<String> checkpointIds = createMultipleCheckpoints(3, false, false);

        List<CheckpointInfo> checkpoints = RocksCheckpointer.getCheckpoints(store.name(), checkpointStore);
        int totalCheckpoints = checkpoints.size();
        // there is an additional null checkpoint
        assertTrue(checkpoints.size() == checkpointIds.size() + 1);

        // Last checkpoint should be null checkpoint
        assertTrue(checkpoints.get(totalCheckpoints - 1).getMetadata() == null);

        // checkpoints are in reverse order
        for (int i = 0; i < checkpointIds.size(); i++) {
            assertEquals(checkpointIds.get(i), checkpoints.get(totalCheckpoints - 2 - i).getId());
        }
    }

    InputStream getBlockedStream(TestStateStore testStore, String path, Duration blockFor) throws IOException {
        InputStream stream = testStore.getCheckpointStore().openInputStream(path);


        FilterInputStream res = new FilterInputStream(stream) {
            @Override
            public synchronized int read(byte[] b, int off, int len) throws IOException {
                try {
                    Thread.sleep(blockFor.toMillis());
                } catch (InterruptedException e) {
                }
                return super.read(b, off, len);
            }
        };
        return res;

    }

    @Test(timeout = 20000)
    public void testRestoreBlockedSlow() throws Exception {
        final int numKvs = 100;
        TestStateStore testStore = Mockito.spy(new TestStateStore(
            runtime.getMethodName(), localDir, remoteDir, true, false));

        store.close();

        testStore.enableCheckpoints(true);
        testStore.setCheckpointRestoreIdleWait(Duration.ofSeconds(3));

        testStore.init();

        testStore.addNumKVs("transaction-1", numKvs, 0);
        // create base checkpoint
        String baseCheckpoint = testStore.checkpoint("checkpoint-1");
        testStore.close();

        String dbName = runtime.getMethodName();

        CheckpointInfo checkpoint = testStore.getLatestCheckpoint();
        CheckpointStore mockCheckpointStore = Mockito.spy(testStore.getCheckpointStore());
        File dbPath = localDir;

        List<CheckpointFile> files = CheckpointFile.list(testStore.getLocalCheckpointsDir(),
            checkpoint.getMetadata());
        String testFile = files.get(0).getRemotePath(dbName, checkpoint.getId(), true);

        // We wait for 10 sec for some data to show up. We will add the data after 8 sec. So the restore should succeed.
        Mockito.doReturn(getBlockedStream(testStore, testFile, Duration.ofSeconds(2)))
            .when(mockCheckpointStore)
            .openInputStream(testFile);

        Mockito.doReturn(mockCheckpointStore).when(testStore).newCheckpointStore();

        try {
            testStore.restore();
        } catch (Exception e) {
            fail("restore should succeed from slow stream");
        }
    }

    @Test(timeout = 20000)
    public void testRestoreBlockedTimeout() throws Exception {
        final int numKvs = 100;
        TestStateStore testStore = Mockito.spy(new TestStateStore(
            runtime.getMethodName(), localDir, remoteDir, true, false));

        store.close();

        testStore.enableCheckpoints(true);
        testStore.setCheckpointRestoreIdleWait(Duration.ofSeconds(10));

        testStore.init();

        testStore.addNumKVs("transaction-1", numKvs, 0);
        // create base checkpoint
        String baseCheckpoint = testStore.checkpoint("checkpoint-1");
        testStore.close();

        String dbName = runtime.getMethodName();

        CheckpointInfo checkpoint = testStore.getLatestCheckpoint();
        CheckpointStore mockCheckpointStore = Mockito.spy(testStore.getCheckpointStore());
        File dbPath = localDir;

        List<CheckpointFile> files = CheckpointFile.list(testStore.getLocalCheckpointsDir(),
            checkpoint.getMetadata());
        String testFile = files.get(0).getRemotePath(dbName, checkpoint.getId(), true);

        Mockito.doReturn(getBlockedStream(testStore, testFile, Duration.ofSeconds(20)))
            .when(mockCheckpointStore)
            .openInputStream(testFile);

        Mockito.doReturn(mockCheckpointStore).when(testStore).newCheckpointStore();

        try {
            testStore.restore();
            fail("should Fail to restore from a blocked stream");
        } catch (Exception e) {

        }
    }

    @Test
    public void testStaleSSTFile() throws Exception {
        final int numKvs = 100;
        TestStateStore testStore = new TestStateStore(
            runtime.getMethodName(), localDir, remoteDir, true, false);

        store.close();

        testStore.enableCheckpoints(true);
        testStore.init();

        testStore.addNumKVs("transaction-1", numKvs, 0);
        // create base checkpoint
        String baseCheckpoint = testStore.checkpoint("checkpoint-1");
        testStore.restore();

        testStore.addNumKVs("transaction-2", numKvs, 100);
        // create failed checkpoint
        String failedCheckpoint = testStore.checkpoint("checkpoint-2");
        // Remove metadata from the checkpoint to signal failure

        CheckpointInfo checkpoint = testStore.getLatestCheckpoint();
        testStore.corruptCheckpoint(checkpoint);

        // restore : this should restore from base checkpoint
        testStore.destroyLocal();
        testStore.restore();
        assertEquals("transaction-1", testStore.get("transaction-id"));

        testStore.addNumKVs("transaction-3", numKvs * 3, 200);
        // create another test checkpoint
        String newCheckpoint = testStore.checkpoint("checkpoint-3");

        // Ensure latest checkpoint can be restored.
        testStore.destroyLocal();
        testStore.restore();
        assertEquals("transaction-3", testStore.get("transaction-id"));
    }

    @Test
    public void testRestoreDowngradeWithoutChecksumCompatible() throws Exception {
        store.close();
        TestStateStore testStore = new TestStateStore(
            runtime.getMethodName(), localDir, remoteDir, true, false);

        testStore.checkpointChecksumCompatible(true);
        downgradeWithoutChecksum(testStore);
        // if compatible then downgrade should work
        assertEquals("transaction-1", testStore.get("transaction-id"));
    }

    @Test
    public void testRestoreDowngradeWithoutChecksumNonCompatible() throws Exception {
        store.close();
        TestStateStore testStore = new TestStateStore(
            runtime.getMethodName(), localDir, remoteDir, true, false);

        testStore.checkpointChecksumCompatible(false);
        try {
            downgradeWithoutChecksum(testStore);
            fail("Downgrade without checksum compatible set should fail");
        } catch (StateStoreException sse) {
        }
    }

    private String downgradeWithoutChecksum(TestStateStore testStore) throws Exception {
        final int numKvs = 100;
        testStore.init();

        testStore.addNumKVs("transaction-1", numKvs, 0);
        String checkpoint1 = testStore.checkpoint("checkpoint-1");

        assertEquals("transaction-1", testStore.get("transaction-id"));

        // restore without checksum information
        RocksdbRestoreTask restoreTask = getDowngradeRestoreTask();

        testStore.destroyLocal();
        CheckpointInfo latest = testStore.getLatestCheckpoint();

        latest.restore(localDir, restoreTask);

        testStore.init();
        return checkpoint1;
    }

    private RocksdbRestoreTask getDowngradeRestoreTask() {
        return new RocksdbRestoreTask(
            runtime.getMethodName(), localCheckpointsDir, checkpointStore) {

            @Override
            protected List<CheckpointFile> getCheckpointFiles(File checkpointedDir, CheckpointMetadata metadata) {
                // ignore the checksum info in metadata
                return metadata.getFilesList().stream()
                    .map(f -> CheckpointFile.builder()
                        .file(checkpointedDir, f)
                        .build())
                    .collect(Collectors.toList());
            }
        };
    }

    @Test
    public void testRestoreOldCheckpointWithoutChecksum() throws Exception {
        final int numKvs = 100;
        TestStateStore testStore = new TestStateStore(
            runtime.getMethodName(), localDir, remoteDir, true, false);

        store.close();

        testStore.enableCheckpoints(true);
        testStore.checkpointChecksumEnable(false);
        testStore.init();

        testStore.addNumKVs("transaction-1", numKvs, 0);
        String checkpoint1 = testStore.checkpoint("checkpoint-1");


        testStore.checkpointChecksumEnable(true);
        // restore : this should restore to checkpoint-2
        testStore.destroyLocal();
        testStore.restore();
        assertEquals("transaction-1", testStore.get("transaction-id"));
    }

    @Test
    public void testRestoreCheckpointMissingLocally() throws Exception {
        // create 3 checkpoints and leave them locally
        List<String> checkpointIds = createMultipleCheckpoints(3, false, false);
        store.close();

        List<String> remoteCheckpoints = checkpointStore.listFiles(RocksUtils.getDestCheckpointsPath(store.name()));
        assertEquals(checkpointIds.size(), remoteCheckpoints.size());

        for (String checkpoint : checkpointIds) {
            assertTrue(remoteCheckpoints.contains(checkpoint));
            assertTrue(new File(localCheckpointsDir, checkpoint).exists());
        }

        // remove a local checkpoint directory
        MoreFiles.deleteRecursively(
            Paths.get(localCheckpointsDir.getAbsolutePath(), checkpointIds.get(2)),
            RecursiveDeleteOption.ALLOW_INSECURE);

        // restore from checkpoints
        CheckpointMetadata metadata = RocksCheckpointer.restore(
            store.name(),
            localDir,
            checkpointStore);
        assertNotNull(metadata);
        assertArrayEquals("checkpoint-2".getBytes(UTF_8), metadata.getTxid().toByteArray());

        for (int i = 0; i < 3; i++) {
            String checkpoint = checkpointIds.get(i);
            if (i == 2) {
                assertTrue(new File(localCheckpointsDir, checkpoint).exists());
            } else {
                assertFalse(new File(localCheckpointsDir, checkpoint).exists());
            }
            assertTrue(
                checkpointStore.fileExists(RocksUtils.getDestCheckpointPath(store.name(), checkpoint)));
        }

        // restore from the latest checkpoint
        store = new RocksdbKVStore<>();
        store.init(spec);

        verifyNumKvs(300);
    }

    @Test
    public void testRestoreLocalCheckpointCorrupted() throws Exception {
        // create 1 checkpoints and leave them locally
        List<String> checkpointIds = createMultipleCheckpoints(1, false, false);
        store.close();

        List<String> remoteCheckpoints = checkpointStore.listFiles(RocksUtils.getDestCheckpointsPath(store.name()));
        assertEquals(checkpointIds.size(), remoteCheckpoints.size());

        for (String checkpoint : checkpointIds) {
            assertTrue(remoteCheckpoints.contains(checkpoint));
            assertTrue(new File(localCheckpointsDir, checkpoint).exists());
        }

        // remove a local checkpoint directory
        File[] files = new File(localCheckpointsDir, checkpointIds.get(0)).listFiles();
        for (int i = 0; i < files.length / 2; i++) {
            assertTrue(files[i].delete());
        }

        // restore from checkpoints
        CheckpointMetadata metadata = RocksCheckpointer.restore(
            store.name(),
            localDir,
            checkpointStore);
        assertNotNull(metadata);
        assertArrayEquals("checkpoint-0".getBytes(UTF_8), metadata.getTxid().toByteArray());

        String checkpoint = checkpointIds.get(0);
        assertTrue(new File(localCheckpointsDir, checkpoint).exists());
        assertTrue(
            checkpointStore.fileExists(RocksUtils.getDestCheckpointPath(store.name(), checkpoint)));

        // restore from the latest checkpoint
        store = new RocksdbKVStore<>();
        store.init(spec);

        verifyNumKvs(100);
    }

    /*
    Bookie can crash or get killed by an operator/automation at any point for any reason.
    This test covers the situation when this happens mid-checkpoint.
     */
    @Test
    public void testCheckpointRestoreAfterCrash() throws Exception {

        final int numGoodCheckpoints = 3;
        createMultipleCheckpoints(numGoodCheckpoints, false, false);

        final int numKvs = 100;
        final String dbName = runtime.getMethodName();
        final byte[] txid = runtime.getMethodName().getBytes(UTF_8);

        // first prepare rocksdb with 100 kvs;
        writeNumKvs(numKvs, 100 * numGoodCheckpoints);

        // create a checkpoint with corrupt metadata
        Checkpoint checkpoint = Checkpoint.create(store.getDb());

        // checkpoint
        RocksdbCheckpointTask checkpointTask = new RocksdbCheckpointTask(
                dbName,
                checkpoint,
                localCheckpointsDir,
                checkpointStore,
                false,
                false,
                true,
                false);

        // let's simulate the crash.
        // crash happens after the createDirectories() succeeded but before
        // the finalizeCheckpoint() completes.
        final AtomicReference<String> idRef = new AtomicReference<>();
        checkpointTask.setInjectedError((id) -> {
            idRef.set(id);
            throw new RuntimeException("test");
        });

        try {
            checkpointTask.checkpoint(txid);
            fail("expected RuntimeException");
        } catch (RuntimeException se) {
            // noop
            // in real life case ths is simply crash,
            // so "finally" at the checkpoint() won't run either
        }

        // remove local checkpointed dir
        File checkpointedDir = new File(localCheckpointsDir, idRef.get());
        MoreFiles.deleteRecursively(
                Paths.get(checkpointedDir.getAbsolutePath()),
                RecursiveDeleteOption.ALLOW_INSECURE);
        assertFalse(checkpointedDir.exists());
        store.close();

        // restore the checkpoint
        RocksCheckpointer.restore(dbName, localCheckpointsDir, checkpointStore);

        // al of the following succeeds if the exception from RocksCheckpointer.restore
        // is ignored

        // make sure all the kvs are readable
        store = new RocksdbKVStore<>();
        store.init(spec);

        verifyNumKvs((numGoodCheckpoints + 1) * numKvs);
        writeNumKvs(numKvs, (numGoodCheckpoints + 1) * numKvs);
        verifyNumKvs((numGoodCheckpoints + 2) * numKvs);
    }

}
