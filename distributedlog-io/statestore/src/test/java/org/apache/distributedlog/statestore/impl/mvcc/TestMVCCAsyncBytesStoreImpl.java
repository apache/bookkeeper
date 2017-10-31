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

package org.apache.distributedlog.statestore.impl.mvcc;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.TestDistributedLogBase;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.common.coder.ByteArrayCoder;
import org.apache.distributedlog.common.concurrent.FutureUtils;
import org.apache.distributedlog.statestore.api.StateStoreSpec;
import org.apache.distributedlog.statestore.api.mvcc.KVRecord;
import org.apache.distributedlog.statestore.api.mvcc.op.PutOp;
import org.apache.distributedlog.statestore.api.mvcc.op.RangeOp;
import org.apache.distributedlog.statestore.api.mvcc.result.Code;
import org.apache.distributedlog.statestore.api.mvcc.result.PutResult;
import org.apache.distributedlog.statestore.api.mvcc.result.RangeResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test of {@link MVCCAsyncBytesStoreImpl}.
 */
@Slf4j
public class TestMVCCAsyncBytesStoreImpl extends TestDistributedLogBase {

    public final TestName runtime = new TestName();

    private static URI uri;
    private static Namespace namespace;

    @BeforeClass
    public static void setupCluster() throws Exception {
        TestDistributedLogBase.setupCluster();
        uri = DLMTestUtil.createDLMURI(zkPort, "/mvcc");
        conf.setPeriodicFlushFrequencyMilliSeconds(2);
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

        tempDir = Files.createTempDir();

        store = new MVCCAsyncBytesStoreImpl(
            () -> new MVCCStoreImpl<>(),
            () -> namespace);
    }

    private StateStoreSpec initSpec(String streamName) {
        return StateStoreSpec.newBuilder()
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
        if (null != tempDir) {
            FileUtils.deleteDirectory(tempDir);
        }
        super.teardown();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitMissingStreamName() throws Exception {
        this.streamName = "test-init-missing-stream-name";
        StateStoreSpec spec = StateStoreSpec.newBuilder()
            .name(streamName)
            .keyCoder(ByteArrayCoder.of())
            .valCoder(ByteArrayCoder.of())
            .localStateStoreDir(tempDir)
            .build();
        FutureUtils.result(store.init(spec));
    }

    @Test
    public void testInit() throws Exception {
        this.streamName = "test-init";
        StateStoreSpec spec = initSpec(streamName);
        FutureUtils.result(store.init(spec));
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
        return FutureUtils.result(FutureUtils.collect(results));
    }

    private CompletableFuture<PutResult<byte[], byte[]>> writeKV(int i, boolean prevKv) {
        PutOp<byte[], byte[]> op = store.getOpFactory().buildPutOp()
            .key(getKey(i))
            .value(getValue(i))
            .prevKV(prevKv)
            .build();
        return store.put(op);
    }

    @Test
    public void testPutKvsAndRange() throws Exception {
        this.streamName = "test-put-kvs";
        StateStoreSpec spec = initSpec(streamName);
        FutureUtils.result(store.init(spec));
        int numPairs = 100;
        List<PutResult<byte[], byte[]>> kvs = writeKVs(numPairs, true);
        assertEquals(numPairs, kvs.size());

        for (PutResult<byte[], byte[]> kv : kvs) {
            assertEquals(Code.OK, kv.code());
            assertNull(kv.prevKV());
            kv.recycle();
        }

        RangeOp<byte[], byte[]> op = store.getOpFactory().buildRangeOp()
            .key(getKey(20))
            .endKey(getKey(79))
            .limit(100)
            .build();
        RangeResult<byte[], byte[]> result = FutureUtils.result(store.range(op));
        assertEquals(Code.OK, result.code());
        assertEquals(60, result.count());
        assertEquals(60, result.kvs().size());
        assertEquals(false, result.hasMore());
        int idx = 20;
        for (KVRecord<byte[], byte[]> record : result.kvs()) {
            assertArrayEquals(getKey(idx), record.key());
            assertArrayEquals(getValue(idx), record.value());
            // revision - starts from 1, but the first revision is used for nop barrier record.
            assertEquals(idx + 2, record.createRevision());
            assertEquals(idx + 2, record.modifiedRevision());
            assertEquals(0, record.version());
            ++idx;
        }
        assertEquals(80, idx);
        result.recycle();
    }

}
