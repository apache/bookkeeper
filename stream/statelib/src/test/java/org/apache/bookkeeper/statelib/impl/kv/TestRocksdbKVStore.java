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

package org.apache.bookkeeper.statelib.impl.kv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import org.apache.bookkeeper.common.coder.StringUtf8Coder;
import org.apache.bookkeeper.common.kv.KV;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.api.exceptions.InvalidStateStoreException;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreRuntimeException;
import org.apache.bookkeeper.statelib.api.kv.KVIterator;
import org.apache.bookkeeper.statelib.api.kv.KVMulti;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test of {@link RocksdbKVStore}.
 */
public class TestRocksdbKVStore {

    @Rule
    public final TestName runtime = new TestName();

    private File tempDir;
    private StateStoreSpec spec;
    private RocksdbKVStore<String, String> store;

    @Before
    public void setUp() throws Exception {
        tempDir = java.nio.file.Files.createTempDirectory("test").toFile();
        spec = StateStoreSpec.builder()
            .name(runtime.getMethodName())
            .keyCoder(StringUtf8Coder.of())
            .valCoder(StringUtf8Coder.of())
            .localStateStoreDir(tempDir)
            .stream(runtime.getMethodName())
            .build();
        store = new RocksdbKVStore<>();
    }

    @After
    public void tearDown() throws Exception {
        if (null != store) {
            store.close();
        }
        if (null != tempDir) {
            FileUtils.deleteDirectory(tempDir);
        }
    }

    //
    // Invalid Arguments Test Cases
    //

    @Test(expected = NullPointerException.class)
    public void testNullPut() throws Exception {
        store.init(spec);
        store.put(null, "val");
    }

    @Test(expected = NullPointerException.class)
    public void testNullPutIfAbsent() throws Exception {
        store.init(spec);
        store.putIfAbsent(null, "val");
    }

    @Test(expected = NullPointerException.class)
    public void testNullGet() throws Exception {
        store.init(spec);
        store.get(null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullDelete() throws Exception {
        store.init(spec);
        store.delete(null);
    }

    @Test(expected = NullPointerException.class)
    public void testMultiNullPut() throws Exception {
        store.init(spec);
        KVMulti<String, String> multi = store.multi();
        multi.put(null, "val");
    }

    @Test(expected = NullPointerException.class)
    public void testMultiNullDelete() throws Exception {
        store.init(spec);
        KVMulti<String, String> multi = store.multi();
        multi.delete(null);
    }

    @Test(expected = NullPointerException.class)
    public void testMultiNullDeleteRangeFrom() throws Exception {
        store.init(spec);
        KVMulti<String, String> multi = store.multi();
        multi.deleteRange(null, "to");
    }

    @Test(expected = NullPointerException.class)
    public void testMultiNullDeleteRangeTo() throws Exception {
        store.init(spec);
        KVMulti<String, String> multi = store.multi();
        multi.deleteRange("from", null);
    }

    @Test(expected = NullPointerException.class)
    public void testMultiNullDeleteRangeBoth() throws Exception {
        store.init(spec);
        KVMulti<String, String> multi = store.multi();
        multi.deleteRange(null, null);
    }

    //
    // Invalid State Test Cases
    //

    @Test(expected = InvalidStateStoreException.class)
    public void testPutBeforeInit() throws Exception {
        store.put("key", "val");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testPutIfAbsentBeforeInit() throws Exception {
        store.putIfAbsent("key", "val");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testGetBeforeInit() throws Exception {
        store.get("key");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testDeleteBeforeInit() throws Exception {
        store.delete("key");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testRangeBeforeInit() throws Exception {
        store.range("key1", "key2");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testMultiBeforeInit() throws Exception {
        store.multi();
    }

    private void initThenClose() throws Exception {
        store.init(spec);
        store.close();
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testPutAfterClose() throws Exception {
        initThenClose();
        store.put("key", "val");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testPutIfAbsentAfterClose() throws Exception {
        initThenClose();
        store.putIfAbsent("key", "val");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testGetAfterClose() throws Exception {
        initThenClose();
        store.get("key");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testDeleteAfterClose() throws Exception {
        initThenClose();
        store.delete("key");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testRangeAfterClose() throws Exception {
        initThenClose();
        store.range("key1", "key2");
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testMultiAfterClose() throws Exception {
        initThenClose();
        store.multi();
    }

    @Test(expected = InvalidStateStoreException.class)
    public void testMultiExecutionAfterClose() throws Exception {
        store.init(spec);
        KVMulti<String, String> multi = store.multi();
        multi.put("key", "value");
        multi.delete("key");
        store.close();
        multi.execute();
    }

    @Test
    public void testPutAfterMultiExecuted() throws Exception {
        store.init(spec);
        KVMulti<String, String> multi = store.multi();
        multi.execute();
        try {
            multi.put("key", "value");
            fail("Should fail put if a multi op is already executed");
        } catch (StateStoreRuntimeException sse) {
        }
    }

    @Test
    public void testDeleteRangeAfterMultiExecuted() throws Exception {
        store.init(spec);
        KVMulti<String, String> multi = store.multi();
        multi.execute();
        try {
            multi.deleteRange("from", "to");
            fail("Should fail put if a multi op is already executed");
        } catch (StateStoreRuntimeException sse) {
        }
    }

    @Test
    public void testDeleteAfterMultiExecuted() throws Exception {
        store.init(spec);
        KVMulti<String, String> multi = store.multi();
        multi.execute();
        try {
            multi.delete("key");
            fail("Should fail put if a multi op is already executed");
        } catch (StateStoreRuntimeException sse) {
        }
    }

    //
    // Operations
    //

    @Test
    public void testGetNull() throws Exception {
        store.init(spec);
        assertNull(store.get("key"));
    }

    @Test
    public void testGetValue() throws Exception {
        store.init(spec);
        store.put("key", "value");
        assertEquals("value", store.get("key"));
    }

    private void writeKVs(int numPairs) {
        KVMulti<String, String> multi = store.multi();
        for (int i = 0; i < numPairs; i++) {
            multi.put("key-" + i, "value-" + i);
        }
        multi.execute();
    }

    @Test
    public void testAllRange() throws Exception {
        store.init(spec);
        writeKVs(10);
        KVIterator<String, String> iter = store.range(null, null);
        int idx = 0;
        while (iter.hasNext()) {
            KV<String, String> kv = iter.next();
            assertEquals("key-" + idx, kv.key());
            assertEquals("value-" + idx, kv.value());
            ++idx;
        }
        assertEquals(10, idx);
        iter.close();
    }

    @Test
    public void testHeadRange() throws Exception {
        store.init(spec);
        writeKVs(10);
        KVIterator<String, String> iter = store.range(null, "key-" + 5);
        int idx = 0;
        while (iter.hasNext()) {
            KV<String, String> kv = iter.next();
            assertEquals("key-" + idx, kv.key());
            assertEquals("value-" + idx, kv.value());
            ++idx;
        }
        assertEquals(6, idx);
        iter.close();
    }

    @Test
    public void testTailRange() throws Exception {
        store.init(spec);
        writeKVs(10);
        KVIterator<String, String> iter = store.range("key-" + 5, null);
        int idx = 5;
        while (iter.hasNext()) {
            KV<String, String> kv = iter.next();
            assertEquals("key-" + idx, kv.key());
            assertEquals("value-" + idx, kv.value());
            ++idx;
        }
        assertEquals(10, idx);
        iter.close();
    }

    @Test
    public void testSubRange() throws Exception {
        store.init(spec);
        writeKVs(10);
        KVIterator<String, String> iter = store.range("key-" + 5, "key-" + 7);
        int idx = 5;
        while (iter.hasNext()) {
            KV<String, String> kv = iter.next();
            assertEquals("key-" + idx, kv.key());
            assertEquals("value-" + idx, kv.value());
            ++idx;
        }
        assertEquals(8, idx);
        iter.close();
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        store.init(spec);
        assertNull(store.putIfAbsent("key", "value"));
        assertEquals("value", store.get("key"));
        assertEquals("value", store.putIfAbsent("key", "another-value"));
        assertEquals("value", store.putIfAbsent("key", null));
    }

    @Test
    public void testDelete() throws Exception {
        store.init(spec);
        store.put("key", "value");
        assertEquals("value", store.get("key"));
        assertEquals("value", store.delete("key"));
        assertNull(store.get("key"));
    }

    @Test
    public void testPutNull() throws Exception {
        store.init(spec);
        store.put("key", "value");
        assertEquals("value", store.get("key"));
        store.put("key", null);
        assertNull(store.get("key"));
    }

}
