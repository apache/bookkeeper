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
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Cleanup;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.kv.KV;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.api.checkpoint.CheckpointStore;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreException;
import org.apache.bookkeeper.statelib.api.kv.KVIterator;
import org.apache.bookkeeper.statelib.impl.kv.RocksdbKVStore;
import org.apache.bookkeeper.statelib.impl.rocksdb.RocksUtils;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.fs.FSCheckpointManager;
import org.apache.bookkeeper.stream.proto.kv.store.CheckpointMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.rocksdb.Checkpoint;

/**
 * Unit test of {@link RocksCheckpointer}.
 */
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
        String[] files = checkpointedDir.list();
        assertNotNull(files);
        assertEquals(files.length, metadata.getFilesCount());
        Set<String> fileSet = Sets.newHashSet();
        for (String file : files) {
            fileSet.add(file);
        }
        for (String file : metadata.getFilesList()) {
            assertTrue(fileSet.contains(file));
        }
    }

    private void verifyRemoteFiles(String checkpointId, File checkpointedDir) throws Exception {
        File[] files = checkpointedDir.listFiles();
        assertNotNull(files);

        for (File file : files) {
            String remoteFile;
            if (RocksUtils.isSstFile(file)) {
                remoteFile = RocksUtils.getDestSstPath(store.name(), file.getName());
            } else {
                remoteFile = RocksUtils.getDestPath(store.name(), checkpointId, file);
            }
            verifyRemoteFile(remoteFile, file);
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
            removeRemoteCheckpointsAfterCheckpoint)) {
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
