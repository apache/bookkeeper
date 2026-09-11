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
package org.apache.bookkeeper.stream.storage.impl.kv;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.kv.KeyValue;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare.CompareResult;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare.CompareTarget;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.ResponseOp;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCAsyncStoreTestBase;
import org.junit.Test;

/**
 * Unit test of {@link TableStoreImpl}.
 */
@CustomLog
public class TableStoreImplTest extends MVCCAsyncStoreTestBase {

    private static final long SC_ID = 123L;
    private static final byte[] RKEY = "routing-key".getBytes(StandardCharsets.UTF_8);

    private static RoutingHeader newHeader() {
        return new RoutingHeader()
            .setRangeId(1234L)
            .setRKey(RKEY)
            .setStreamId(1256L);
    }

    private static final RoutingHeader HEADER = newHeader();

    private TableStoreImpl tableStore;

    @Override
    protected void doSetup() throws Exception {
        this.tableStore = new TableStoreImpl(store);
    }

    @Override
    protected void doTeardown() throws Exception {
    }

    //
    // Put & Range Ops
    //

    private static byte[] getKey(int i) {
        return String.format("key-%05d", i).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] getValue(int i) {
        return String.format("value-%05d", i).getBytes(StandardCharsets.UTF_8);
    }

    private List<KeyValue> writeKVs(int numPairs, boolean prevKv) throws Exception {
        List<CompletableFuture<PutResponse>> results =
            Lists.newArrayListWithExpectedSize(numPairs);
        for (int i = 0; i < numPairs; i++) {
            results.add(writeKV(i, prevKv));
        }
        List<PutResponse> responses = result(FutureUtils.collect(results));
        List<KeyValue> kvs = new ArrayList<>(responses.size());
        for (PutResponse putResp : responses) {
            assertEquals(StatusCode.SUCCESS, putResp.getHeader().getCode());
            assertEquals(HEADER, putResp.getHeader().getRoutingHeader());
            if (putResp.hasPrevKv()) {
                kvs.add(putResp.getPrevKv());
            } else {
                kvs.add(null);
            }
        }
        return kvs;
    }

    private CompletableFuture<PutResponse> writeKV(int i, boolean prevKv) {
        PutRequest req = new PutRequest()
            .setKey(getKey(i))
            .setValue(getValue(i))
            .setPrevKv(prevKv);
        req.setHeader().copyFrom(HEADER);
        return tableStore.put(req);
    }

    RangeResponse getKeyFromTableStore(int i) throws Exception {
        RangeRequest req = new RangeRequest()
            .setKey(getKey(i));
        req.setHeader().copyFrom(HEADER);
        return result(tableStore.range(req));
    }

    KeyValue getKeyValue(int i) throws Exception {
        RangeResponse rr = getKeyFromTableStore(i);
        assertEquals(StatusCode.SUCCESS, rr.getHeader().getCode());
        assertEquals(HEADER, rr.getHeader().getRoutingHeader());
        assertFalse(rr.isMore());
        if (0 == rr.getCount()) {
            return null;
        } else {
            return rr.getKvAt(0);
        }
    }

    void putKeyToTableStore(int key, int value) throws Exception {
        PutRequest req = new PutRequest()
            .setKey(getKey(key))
            .setValue(getValue(value));
        req.setHeader().copyFrom(HEADER);
        PutResponse putResp = result(tableStore.put(req));

        assertEquals(StatusCode.SUCCESS, putResp.getHeader().getCode());
        assertEquals(HEADER, putResp.getHeader().getRoutingHeader());
        assertFalse(putResp.hasPrevKv());
    }

    KeyValue putIfAbsentToTableStore(int key, int value, boolean expectedSuccess) throws Exception {
        TxnRequest req = new TxnRequest();
        req.setHeader().copyFrom(HEADER);

        Compare compare = req.addCompare();
        compare.setResult(CompareResult.EQUAL);
        compare.setTarget(CompareTarget.VALUE);
        compare.setKey(getKey(key));
        compare.setValue(new byte[0]);

        PutRequest putReq = new PutRequest()
            .setKey(getKey(key))
            .setValue(getValue(value))
            .setPrevKv(true);
        putReq.setHeader().copyFrom(HEADER);
        req.addSuccess().setRequestPut().copyFrom(putReq);

        RangeRequest rangeReq = new RangeRequest()
            .setKey(getKey(key));
        rangeReq.setHeader().copyFrom(HEADER);
        req.addFailure().setRequestRange().copyFrom(rangeReq);

        TxnResponse txnResp = result(tableStore.txn(req));

        assertEquals(HEADER, txnResp.getHeader().getRoutingHeader());
        assertEquals(StatusCode.SUCCESS, txnResp.getHeader().getCode());

        ResponseOp respOp = txnResp.getResponseAt(0);
        if (expectedSuccess) {
            assertTrue(txnResp.isSucceeded());
            PutResponse putResp = respOp.getResponsePut();
            assertEquals(HEADER, putResp.getHeader().getRoutingHeader());
            if (!putResp.hasPrevKv()) {
                return null;
            }
            return putResp.getPrevKv();
        } else {
            assertFalse(txnResp.isSucceeded());
            RangeResponse rangeResp = respOp.getResponseRange();
            if (rangeResp.getCount() == 0) {
                return null;
            } else {
                assertEquals(1, rangeResp.getCount());
                return rangeResp.getKvAt(0);
            }
        }
    }

    TxnResponse vPutToTableStore(int key, int value, long version)
        throws Exception {
        TxnRequest req = new TxnRequest();
        req.setHeader().copyFrom(HEADER);

        Compare compare = req.addCompare();
        compare.setResult(CompareResult.EQUAL);
        compare.setTarget(CompareTarget.VERSION);
        compare.setKey(getKey(key));
        compare.setVersion(version);

        PutRequest putReq = new PutRequest()
            .setKey(getKey(key))
            .setValue(getValue(value))
            .setPrevKv(true);
        putReq.setHeader().copyFrom(HEADER);
        req.addSuccess().setRequestPut().copyFrom(putReq);

        RangeRequest rangeReq = new RangeRequest()
            .setKey(getKey(key));
        rangeReq.setHeader().copyFrom(HEADER);
        req.addFailure().setRequestRange().copyFrom(rangeReq);

        return result(tableStore.txn(req));
    }

    KeyValue verifyVPutResponse(TxnResponse txnResp, boolean expectedSuccess) throws Exception {
        assertEquals(HEADER, txnResp.getHeader().getRoutingHeader());
        assertEquals(StatusCode.SUCCESS, txnResp.getHeader().getCode());

        ResponseOp respOp = txnResp.getResponseAt(0);
        if (expectedSuccess) {
            assertTrue(txnResp.isSucceeded());
            PutResponse putResp = respOp.getResponsePut();
            assertEquals(HEADER, putResp.getHeader().getRoutingHeader());
            if (!putResp.hasPrevKv()) {
                return null;
            }
            return putResp.getPrevKv();
        } else {
            assertFalse(txnResp.isSucceeded());
            RangeResponse rangeResp = respOp.getResponseRange();
            if (rangeResp.getCount() == 0) {
                return null;
            } else {
                assertEquals(1, rangeResp.getCount());
                return rangeResp.getKvAt(0);
            }
        }
    }

    TxnResponse rPutToTableStore(int key, int value, long revision)
        throws Exception {
        TxnRequest req = new TxnRequest();
        req.setHeader().copyFrom(HEADER);

        Compare compare = req.addCompare();
        compare.setResult(CompareResult.EQUAL);
        compare.setTarget(CompareTarget.MOD);
        compare.setKey(getKey(key));
        compare.setModRevision(revision);

        PutRequest putReq = new PutRequest()
            .setKey(getKey(key))
            .setValue(getValue(value))
            .setPrevKv(true);
        putReq.setHeader().copyFrom(HEADER);
        req.addSuccess().setRequestPut().copyFrom(putReq);

        RangeRequest rangeReq = new RangeRequest()
            .setKey(getKey(key));
        rangeReq.setHeader().copyFrom(HEADER);
        req.addFailure().setRequestRange().copyFrom(rangeReq);

        return result(tableStore.txn(req));
    }

    KeyValue deleteKeyFromTableStore(int key) throws Exception {
        DeleteRangeRequest req = new DeleteRangeRequest()
            .setKey(getKey(key))
            .setPrevKv(true);
        req.setHeader().copyFrom(HEADER);
        DeleteRangeResponse response = result(tableStore.delete(req));

        assertEquals(StatusCode.SUCCESS, response.getHeader().getCode());
        assertEquals(HEADER, response.getHeader().getRoutingHeader());
        if (0 == response.getPrevKvsCount()) {
            return null;
        } else {
            return response.getPrevKvAt(0);
        }
    }

    List<KeyValue> deleteRange(int startKey, int endKey) throws Exception {
        DeleteRangeRequest req = new DeleteRangeRequest()
            .setKey(getKey(startKey))
            .setRangeEnd(getKey(endKey))
            .setPrevKv(true);
        req.setHeader().copyFrom(HEADER);
        DeleteRangeResponse delResp = result(tableStore.delete(req));

        assertEquals(StatusCode.SUCCESS, delResp.getHeader().getCode());
        assertEquals(HEADER, delResp.getHeader().getRoutingHeader());
        return delResp.getPrevKvsList();
    }

    List<KeyValue> range(int startKey, int endKey) throws Exception {
        RangeRequest req = new RangeRequest()
            .setKey(getKey(startKey))
            .setRangeEnd(getKey(endKey));
        req.setHeader().copyFrom(HEADER);
        RangeResponse rangeResp = result(tableStore.range(req));

        assertEquals(StatusCode.SUCCESS, rangeResp.getHeader().getCode());
        assertEquals(HEADER, rangeResp.getHeader().getRoutingHeader());
        return rangeResp.getKvsList();
    }

    @Test
    public void testBasicOps() throws Exception {
        // normal put
        {
            // get key(0)
            KeyValue kv = getKeyValue(0);
            assertNull(kv);
            // put key(0), value(0)
            putKeyToTableStore(0, 0);
            // get key(0) again
            kv = getKeyValue(0);
            assertArrayEquals(getKey(0), kv.getKey());
            assertArrayEquals(getValue(0), kv.getValue());
        }

        // putIfAbsent
        {
            // failure case
            KeyValue prevKV = putIfAbsentToTableStore(0, 99, false);
            assertNotNull(prevKV);
            assertArrayEquals(getKey(0), prevKV.getKey());
            assertArrayEquals(getValue(0), prevKV.getValue());
            // get key(0)
            KeyValue kv = getKeyValue(0);
            assertArrayEquals(getKey(0), kv.getKey());
            assertArrayEquals(getValue(0), kv.getValue());
            // success case
            prevKV = putIfAbsentToTableStore(1, 1, true);
            assertNull(prevKV);
            // get key(1)
            kv = getKeyValue(1);
            assertArrayEquals(getKey(1), kv.getKey());
            assertArrayEquals(getValue(1), kv.getValue());
        }

        // vPut
        {
            // key-not-found case
            int key = 2;
            int initialVal = 2;
            int casVal = 99;
            TxnResponse response = vPutToTableStore(key, initialVal, 100L);
            assertEquals(StatusCode.KEY_NOT_FOUND, response.getHeader().getCode());

            // vPut(k, v, -1L)
            response = vPutToTableStore(key, initialVal, -1L);
            assertEquals(StatusCode.KEY_NOT_FOUND, response.getHeader().getCode());
            // put(key2, v)
            KeyValue prevKV = putIfAbsentToTableStore(key, initialVal, true);
            assertNull(prevKV);
            // vPut(key2, v, 0)
            response = vPutToTableStore(key, casVal, 0);
            assertEquals(StatusCode.SUCCESS, response.getHeader().getCode());
            prevKV = verifyVPutResponse(response, true);
            assertNotNull(prevKV);
            assertArrayEquals(getKey(key), prevKV.getKey());
            assertArrayEquals(getValue(initialVal), prevKV.getValue());
            assertEquals(0, prevKV.getVersion());

            KeyValue kv = getKeyValue(key);
            assertArrayEquals(getKey(key), kv.getKey());
            assertArrayEquals(getValue(casVal), kv.getValue());
        }

        // rPut
        {
            // key-not-found case
            int key = 3;
            int initialVal = 3;
            int casVal = 99;

            TxnResponse response = rPutToTableStore(key, initialVal, 100L);
            assertEquals(StatusCode.KEY_NOT_FOUND, response.getHeader().getCode());

            // rPut(k, v, -1L)
            response = rPutToTableStore(key, initialVal, -1L);
            assertEquals(StatusCode.KEY_NOT_FOUND, response.getHeader().getCode());

            // put(key2, v)
            KeyValue prevKV = putIfAbsentToTableStore(key, initialVal, true);
            assertNull(prevKV);

            KeyValue kv = getKeyValue(key);
            long revision = kv.getModRevision();
            assertArrayEquals(getValue(initialVal), kv.getValue());

            // rPut(key2, v, 0)
            response = rPutToTableStore(key, casVal, revision);
            assertEquals(StatusCode.SUCCESS, response.getHeader().getCode());

            kv = getKeyValue(key);
            assertEquals(revision + 1, kv.getModRevision());
            assertArrayEquals(getValue(casVal), kv.getValue());
        }

        // delete(k)
        {
            // key not found
            KeyValue kv = deleteKeyFromTableStore(99);
            assertNull(kv);
            // key exists
            int key = 0;
            kv = getKeyValue(key);
            assertArrayEquals(getKey(key), kv.getKey());
            assertArrayEquals(getValue(key), kv.getValue());
            kv = deleteKeyFromTableStore(key);
            assertNotNull(kv);
            assertArrayEquals(getKey(key), kv.getKey());
            assertArrayEquals(getValue(key), kv.getValue());
            // the key/value pair should not exist after deletion.
            kv = getKeyValue(key);
            assertNull(kv);
        }

    }

    @Test
    public void testPutGetDeleteRanges() throws Exception {
        int numPairs = 100;
        List<KeyValue> prevKvs = writeKVs(numPairs, true);
        assertEquals(numPairs, prevKvs.size());

        for (KeyValue prevKv : prevKvs) {
            assertNull(prevKv);
        }

        verifyRange(20, 70, 2, 2, 0);

        prevKvs = deleteRange(20, 70);
        assertNotNull(prevKvs);
        verifyRecords(
            prevKvs,
            20, 70,
            2, 2, 0);

        prevKvs = range(20, 70);
        assertTrue(prevKvs.isEmpty());
    }

    private void verifyRange(int startKey,
                             int endKey,
                             int startCreateRevision,
                             int startModRevision,
                             int expectedVersion) throws Exception {
        int count = endKey - startKey + 1;
        List<KeyValue> kvs = range(startKey, endKey);
        assertEquals(count, kvs.size());

        verifyRecords(kvs, startKey, endKey, startCreateRevision, startModRevision, expectedVersion);
    }

    private void verifyRecords(List<KeyValue> kvs,
                               int startKey, int endKey,
                               int startCreateRevision,
                               int startModRevision,
                               int expectedVersion) {
        int idx = startKey;
        for (KeyValue kv : kvs) {
            assertArrayEquals(getKey(idx), kv.getKey());
            assertArrayEquals(getValue(idx), kv.getValue());
            // revision - starts from 1, but the first revision is used for nop barrier record.
            assertEquals(idx + startCreateRevision, kv.getCreateRevision());
            assertEquals(idx + startModRevision, kv.getModRevision());
            assertEquals(expectedVersion, kv.getVersion());
            ++idx;
        }
        assertEquals(endKey + 1, idx);
    }

}
