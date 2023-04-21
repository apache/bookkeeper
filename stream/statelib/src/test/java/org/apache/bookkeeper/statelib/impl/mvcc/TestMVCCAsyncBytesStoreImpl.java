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

package org.apache.bookkeeper.statelib.impl.mvcc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.op.PutOp;
import org.apache.bookkeeper.api.kv.options.Options;
import org.apache.bookkeeper.api.kv.result.Code;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.api.kv.result.PutResult;
import org.apache.bookkeeper.common.coder.ByteArrayCoder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.api.exceptions.MVCCStoreException;
import org.apache.distributedlog.DLMTestUtil;
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

/**
 * Unit test of {@link MVCCAsyncBytesStoreImpl}.
 */
@Slf4j
public class TestMVCCAsyncBytesStoreImpl extends TestDistributedLogBase {

    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();

    private static URI uri;
    private static Namespace namespace;

    @BeforeClass
    public static void setupCluster() throws Exception {
        TestDistributedLogBase.setupCluster();
        uri = DLMTestUtil.createDLMURI(zkPort, "/mvcc");
        conf.setPeriodicFlushFrequencyMilliSeconds(2);
        conf.setWriteLockEnabled(false);
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

    private String streamName;
    private File tempDir;
    private MVCCAsyncBytesStoreImpl store;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        ensureURICreated(uri);

        tempDir = testDir.newFolder();

        store = new MVCCAsyncBytesStoreImpl(
            () -> new MVCCStoreImpl<>(),
            () -> namespace);
    }

    private StateStoreSpec initSpec(String streamName) {
        return StateStoreSpec.builder()
            .name(streamName)
            .keyCoder(ByteArrayCoder.of())
            .valCoder(ByteArrayCoder.of())
            .stream(streamName)
            .localStateStoreDir(tempDir)
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
        super.teardown();
    }

    @Test(expected = NullPointerException.class)
    public void testInitMissingStreamName() throws Exception {
        this.streamName = "test-init-missing-stream-name";
        StateStoreSpec spec = StateStoreSpec.builder()
            .name(streamName)
            .keyCoder(ByteArrayCoder.of())
            .valCoder(ByteArrayCoder.of())
            .localStateStoreDir(tempDir)
            .build();
        result(store.init(spec));
    }

    @Test
    public void testInit() throws Exception {
        this.streamName = "test-init";
        StateStoreSpec spec = initSpec(streamName);
        result(store.init(spec));
        assertTrue(store.ownWriteScheduler());
        assertFalse(store.ownReadScheduler());
        assertEquals(streamName, store.name());
    }

    //
    // Put & Range Ops
    //

    private byte[] getKey(int i) {
        return String.format("key-%05d", i).getBytes(UTF_8);
    }

    private byte[] getValue(int i) {
        return String.format("value-%05d", i).getBytes(UTF_8);
    }

    private List<PutResult<byte[], byte[]>> writeKVs(int numPairs, boolean prevKv) throws Exception {
        List<CompletableFuture<PutResult<byte[], byte[]>>> results = Lists.newArrayListWithExpectedSize(numPairs);
        for (int i = 0; i < numPairs; i++) {
            results.add(writeKV(i, prevKv));
        }
        return result(FutureUtils.collect(results));
    }

    private CompletableFuture<PutResult<byte[], byte[]>> writeKV(int i, boolean prevKv) {
        PutOp<byte[], byte[]> op = store.getOpFactory().newPut(
            getKey(i),
            getValue(i),
            Options.putAndGet());
        return store.put(op).whenComplete((value, cause) -> op.close());
    }

    @Test
    public void testBasicOps() throws Exception {
        this.streamName = "test-basic-ops";
        StateStoreSpec spec = initSpec(streamName);
        result(store.init(spec));

        // normal put
        {
            assertNull(result(store.get(getKey(0))));
            result(store.put(getKey(0), getValue(0)));
            assertArrayEquals(getValue(0), result(store.get(getKey(0))));
        }

        // putIfAbsent
        {
            // failure case
            assertArrayEquals(getValue(0), result(store.putIfAbsent(getKey(0), getValue(99))));
            assertArrayEquals(getValue(0), result(store.get(getKey(0))));
            // success case
            byte[] key1 = getKey(1);
            assertNull(result(store.putIfAbsent(key1, getValue(1))));
            assertArrayEquals(getValue(1), result(store.get(key1)));
        }

        // vPut
        {
            // key-not-found case
            int key = 2;
            int initialVal = 2;
            int casVal = 99;
            try {
                result(store.vPut(getKey(key), getValue(initialVal), 100L));
                fail("key2 doesn't exist yet");
            } catch (MVCCStoreException e) {
                assertEquals(Code.KEY_NOT_FOUND, e.getCode());
            }
            // vPut(k, v, -1L)
            try {
                result(store.vPut(getKey(key), getValue(initialVal), -1L));
                fail("key2 doesn't exist yet");
            } catch (MVCCStoreException e) {
                assertEquals(Code.KEY_NOT_FOUND, e.getCode());
            }
            // put(key2, v)
            assertNull(result(store.putIfAbsent(getKey(key), getValue(initialVal))));
            // vPut(key2, v, 0)
            assertEquals(1L, result(store.vPut(getKey(key), getValue(casVal), 0)).longValue());
            assertArrayEquals(getValue(casVal), result(store.get(getKey(key))));
        }

        // rPut
        {
            // key-not-found case
            int key = 3;
            int initialVal = 3;
            int casVal = 99;
            try {
                result(store.rPut(getKey(key), getValue(initialVal), 100L));
                fail("key2 doesn't exist yet");
            } catch (MVCCStoreException e) {
                assertEquals(Code.KEY_NOT_FOUND, e.getCode());
            }
            // vPut(k, v, -1L)
            try {
                result(store.rPut(getKey(key), getValue(initialVal), -1L));
                fail("key2 doesn't exist yet");
            } catch (MVCCStoreException e) {
                assertEquals(Code.KEY_NOT_FOUND, e.getCode());
            }
            // put(key2, v)
            assertNull(result(store.putIfAbsent(getKey(key), getValue(initialVal))));
            KeyValue<byte[], byte[]> kv = result(store.getKeyValue(getKey(key)));
            long revision = kv.modifiedRevision();
            assertArrayEquals(getValue(initialVal), kv.value());
            // vPut(key2, v, 0)
            assertEquals(revision + 1, result(store.rPut(getKey(key), getValue(casVal), revision)).longValue());
            assertArrayEquals(getValue(casVal), result(store.get(getKey(key))));
        }

        // delete(k)
        {
            // key not found
            assertNull(result(store.delete(getKey(99))));
            // key exists
            int key = 0;
            assertArrayEquals(getValue(key), result(store.get(getKey(key))));
            assertArrayEquals(getValue(key), result(store.delete(getKey(key))));
            assertNull(result(store.get(getKey(key))));
        }

        // delete(k, v)
        {
            // key not found
            assertNull(result(store.delete(getKey(99))));
            // key exists
            int key = 1;
            assertArrayEquals(getValue(key), result(store.get(getKey(key))));
            assertFalse(result(store.delete(getKey(key), getValue(99))));
            assertArrayEquals(getValue(key), result(store.get(getKey(key))));
            assertTrue(result(store.delete(getKey(key), getValue(key))));
            assertNull(result(store.get(getKey(key))));
        }
        // vDelete
        {
            int key = 2;
            @Cleanup KeyValue<byte[], byte[]> kv = result(store.getKeyValue(getKey(key)));
            long expectedVersion = kv.version();
            try {
                result(store.vDelete(getKey(key), expectedVersion + 1));
                fail("should fail to delete a key with wrong version");
            } catch (MVCCStoreException e) {
                assertEquals(Code.BAD_REVISION, e.getCode());
            }
            // vDelete(k, -1L)
            try {
                result(store.vDelete(getKey(key), -1L));
                fail("Should fail to delete a key with version(-1)");
            } catch (MVCCStoreException e) {
                assertEquals(Code.BAD_REVISION, e.getCode());
            }
            // vDelete(key2, version)
            @Cleanup KeyValue<byte[], byte[]> deletedKv = (result(store.vDelete(getKey(key), expectedVersion)));
            assertNotNull(deletedKv);
            assertEquals(kv.createRevision(), deletedKv.createRevision());
            assertEquals(kv.modifiedRevision(), deletedKv.modifiedRevision());
            assertEquals(kv.version(), deletedKv.version());
            assertArrayEquals(kv.value(), deletedKv.value());
            assertNull(result(store.get(getKey(key))));
        }

        // rPut
        {
            int key = 3;
            @Cleanup KeyValue<byte[], byte[]> kv = result(store.getKeyValue(getKey(key)));
            long expectedRevision = kv.modifiedRevision();
            try {
                result(store.rDelete(getKey(key), expectedRevision + 1));
                fail("should fail to delete a key with wrong revision");
            } catch (MVCCStoreException e) {
                assertEquals(Code.BAD_REVISION, e.getCode());
            }
            // rDelete(k, -1L)
            try {
                result(store.rDelete(getKey(key), -1L));
                fail("Should fail to delete a key with revision(-1)");
            } catch (MVCCStoreException e) {
                assertEquals(Code.BAD_REVISION, e.getCode());
            }
            // rDelete(key2, revision)
            @Cleanup KeyValue<byte[], byte[]> deletedKv = (result(store.rDelete(getKey(key), expectedRevision)));
            assertNotNull(deletedKv);
            assertEquals(kv.createRevision(), deletedKv.createRevision());
            assertEquals(kv.modifiedRevision(), deletedKv.modifiedRevision());
            assertEquals(kv.version(), deletedKv.version());
            assertArrayEquals(kv.value(), deletedKv.value());
            assertNull(result(store.get(getKey(key))));
        }

        // increment failure
        {
            int ki = 3;
            byte[] key = getKey(ki);
            result(store.put(key, getValue(ki)));
            try {
                result(store.increment(key, 100L));
                fail("Can't increment a non-number key");
            } catch (MVCCStoreException e) {
                assertEquals(Code.ILLEGAL_OP, e.getCode());
            }
        }

        // increment success
        {
            int ki = 4;
            byte[] key = getKey(ki);
            for (int i = 0; i < 5; i++) {
                result(store.increment(key, 100L));
                @Cleanup KeyValue<byte[], byte[]> kv = result(store.getKeyValue(key));
                assertEquals(100L * (i + 1), kv.numberValue());
            }
        }
    }

    @Test
    public void testPutGetDeleteRanges() throws Exception {
        this.streamName = "test-put-kvs";
        StateStoreSpec spec = initSpec(streamName);
        result(store.init(spec));
        int numPairs = 100;
        List<PutResult<byte[], byte[]>> kvs = writeKVs(numPairs, true);
        assertEquals(numPairs, kvs.size());

        for (PutResult<byte[], byte[]> kv : kvs) {
            assertEquals(Code.OK, kv.code());
            assertNull(kv.prevKv());
            kv.close();
        }

        verifyRange(20, 70, 2, 2, 0);

        List<KeyValue<byte[], byte[]>> prevKvs = result(store.deleteRange(getKey(20), getKey(70)));
        assertNotNull(prevKvs);
        verifyRecords(
            prevKvs,
            20, 70,
            2, 2, 0);
        prevKvs.forEach(KeyValue::close);

        prevKvs = result(store.range(getKey(20), getKey(70)));
        assertTrue(prevKvs.isEmpty());
    }

    private void verifyRange(int startKey,
                             int endKey,
                             int startCreateRevision,
                             int startModRevision,
                             int expectedVersion) throws Exception {
        int count = endKey - startKey + 1;
        List<KeyValue<byte[], byte[]>> kvs = result(store.range(getKey(startKey), getKey(endKey)));
        assertEquals(count, kvs.size());

        verifyRecords(kvs, startKey, endKey, startCreateRevision, startModRevision, expectedVersion);

        kvs.forEach(KeyValue::close);
    }

    private void verifyRecords(List<KeyValue<byte[], byte[]>> kvs,
                               int startKey, int endKey,
                               int startCreateRevision,
                               int startModRevision,
                               int expectedVersion) {
        int idx = startKey;
        for (KeyValue<byte[], byte[]> record : kvs) {
            assertArrayEquals(getKey(idx), record.key());
            assertArrayEquals(getValue(idx), record.value());
            // revision - starts from 1, but the first revision is used for nop barrier record.
            assertEquals(idx + startCreateRevision, record.createRevision());
            assertEquals(idx + startModRevision, record.modifiedRevision());
            assertEquals(expectedVersion, record.version());
            ++idx;
        }
        assertEquals(endKey + 1, idx);
    }

    @Test
    public void testReplayJournal() throws Exception {
        this.streamName = "test-replay-journal";
        StateStoreSpec spec = initSpec(streamName);
        result(store.init(spec));

        int numKvs = 10;

        // putIfAbsent
        IntStream.range(0, numKvs)
            .forEach(i -> {
                try {
                    result(store.putIfAbsent(getKey(i), getValue(100 + i)));
                } catch (Exception e) {
                    log.error("Failed to put kv pair ({})", i, e);
                }
            });

        log.info("Closing the store '{}' ...", streamName);
        // close the store
        store.close();
        log.info("Closed the store '{}' ...", streamName);

        // open the store again to replay the journal.
        store = new MVCCAsyncBytesStoreImpl(
            () -> new MVCCStoreImpl<>(),
            () -> namespace);
        spec = StateStoreSpec.builder()
            .name(streamName)
            .keyCoder(ByteArrayCoder.of())
            .valCoder(ByteArrayCoder.of())
            .stream(streamName)
            .localStateStoreDir(testDir.newFolder())
            .build();
        result(store.init(spec));

        // verify the key/value pairs
        for (int i = 0; i < numKvs; i++) {
            byte[] value = result(store.get(getKey(i)));
            assertNotNull(value);
            assertArrayEquals(getValue(100 + i), value);
        }
    }

}
