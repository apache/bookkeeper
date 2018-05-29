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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.bookkeeper.stream.proto.kv.rpc.RequestOp;
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
@Slf4j
public class TableStoreImplTest extends MVCCAsyncStoreTestBase {

    private static final long SC_ID = 123L;
    private static final ByteString RKEY = ByteString.copyFromUtf8("routing-key");
    private static final RoutingHeader HEADER = RoutingHeader.newBuilder()
        .setRangeId(1234L)
        .setRKey(RKEY)
        .setStreamId(1256L)
        .build();

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

    private static ByteString getKey(int i) {
        return ByteString.copyFromUtf8(String.format("key-%05d", i));
    }

    private ByteString getValue(int i) {
        return ByteString.copyFromUtf8(String.format("value-%05d", i));
    }

    private List<KeyValue> writeKVs(int numPairs, boolean prevKv) throws Exception {
        List<CompletableFuture<PutResponse>> results =
            Lists.newArrayListWithExpectedSize(numPairs);
        for (int i = 0; i < numPairs; i++) {
            results.add(writeKV(i, prevKv));
        }
        return Lists.transform(
            result(FutureUtils.collect(results)), putResp -> {
                assertEquals(StatusCode.SUCCESS, putResp.getHeader().getCode());
                assertEquals(HEADER, putResp.getHeader().getRoutingHeader());
                if (putResp.hasPrevKv()) {
                    return putResp.getPrevKv();
                } else {
                    return null;
                }
            });
    }

    private CompletableFuture<PutResponse> writeKV(int i, boolean prevKv) {
        return tableStore.put(PutRequest.newBuilder()
            .setKey(getKey(i))
            .setValue(getValue(i))
            .setHeader(HEADER)
            .setPrevKv(prevKv)
            .build());
    }

    RangeResponse getKeyFromTableStore(int i) throws Exception {
        return result(
            tableStore.range(RangeRequest.newBuilder()
                .setHeader(HEADER)
                .setKey(getKey(i))
                .build()));
    }

    KeyValue getKeyValue(int i) throws Exception {
        RangeResponse rr = getKeyFromTableStore(i);
        assertEquals(StatusCode.SUCCESS, rr.getHeader().getCode());
        assertEquals(HEADER, rr.getHeader().getRoutingHeader());
        assertFalse(rr.getMore());
        if (0 == rr.getCount()) {
            return null;
        } else {
            return rr.getKvs(0);
        }
    }

    void putKeyToTableStore(int key, int value) throws Exception {
        PutResponse putResp = result(
            tableStore.put(PutRequest.newBuilder()
                .setHeader(HEADER)
                .setKey(getKey(key))
                .setValue(getValue(value))
                .build()));

        assertEquals(StatusCode.SUCCESS, putResp.getHeader().getCode());
        assertEquals(HEADER, putResp.getHeader().getRoutingHeader());
        assertFalse(putResp.hasPrevKv());
    }

    KeyValue putIfAbsentToTableStore(int key, int value, boolean expectedSuccess) throws Exception {
        TxnResponse txnResp = result(
            tableStore.txn(TxnRequest.newBuilder()
                .setHeader(HEADER)
                .addCompare(Compare.newBuilder()
                    .setResult(CompareResult.EQUAL)
                    .setTarget(CompareTarget.VALUE)
                    .setKey(getKey(key))
                    .setValue(ByteString.copyFrom(new byte[0])))
                .addSuccess(RequestOp.newBuilder()
                    .setRequestPut(PutRequest.newBuilder()
                        .setHeader(HEADER)
                        .setKey(getKey(key))
                        .setValue(getValue(value))
                        .setPrevKv(true)
                        .build()))
                .addFailure(RequestOp.newBuilder()
                    .setRequestRange(RangeRequest.newBuilder()
                        .setHeader(HEADER)
                        .setKey(getKey(key))
                        .build()))
                .build()));

        assertEquals(HEADER, txnResp.getHeader().getRoutingHeader());
        assertEquals(StatusCode.SUCCESS, txnResp.getHeader().getCode());

        ResponseOp respOp = txnResp.getResponses(0);
        if (expectedSuccess) {
            assertTrue(txnResp.getSucceeded());
            PutResponse putResp = respOp.getResponsePut();
            assertEquals(HEADER, putResp.getHeader().getRoutingHeader());
            if (!putResp.hasPrevKv()) {
                return null;
            }
            return putResp.getPrevKv();
        } else {
            assertFalse(txnResp.getSucceeded());
            RangeResponse rangeResp = respOp.getResponseRange();
            if (rangeResp.getCount() == 0) {
                return null;
            } else {
                assertEquals(1, rangeResp.getCount());
                return rangeResp.getKvs(0);
            }
        }
    }

    TxnResponse vPutToTableStore(int key, int value, long version)
        throws Exception {
        return result(
            tableStore.txn(TxnRequest.newBuilder()
                .setHeader(HEADER)
                .addCompare(Compare.newBuilder()
                    .setResult(CompareResult.EQUAL)
                    .setTarget(CompareTarget.VERSION)
                    .setKey(getKey(key))
                    .setVersion(version))
                .addSuccess(RequestOp.newBuilder()
                    .setRequestPut(PutRequest.newBuilder()
                        .setHeader(HEADER)
                        .setKey(getKey(key))
                        .setValue(getValue(value))
                        .setPrevKv(true)
                        .build()))
                .addFailure(RequestOp.newBuilder()
                    .setRequestRange(RangeRequest.newBuilder()
                        .setHeader(HEADER)
                        .setKey(getKey(key))
                        .build()))
                .build()));
    }

    KeyValue verifyVPutResponse(TxnResponse txnResp, boolean expectedSuccess) throws Exception {
        assertEquals(HEADER, txnResp.getHeader().getRoutingHeader());
        assertEquals(StatusCode.SUCCESS, txnResp.getHeader().getCode());

        ResponseOp respOp = txnResp.getResponses(0);
        if (expectedSuccess) {
            assertTrue(txnResp.getSucceeded());
            PutResponse putResp = respOp.getResponsePut();
            assertEquals(HEADER, putResp.getHeader().getRoutingHeader());
            if (!putResp.hasPrevKv()) {
                return null;
            }
            return putResp.getPrevKv();
        } else {
            assertFalse(txnResp.getSucceeded());
            RangeResponse rangeResp = respOp.getResponseRange();
            if (rangeResp.getCount() == 0) {
                return null;
            } else {
                assertEquals(1, rangeResp.getCount());
                return rangeResp.getKvs(0);
            }
        }
    }

    TxnResponse rPutToTableStore(int key, int value, long revision)
        throws Exception {
        return result(
            tableStore.txn(TxnRequest.newBuilder()
                .setHeader(HEADER)
                .addCompare(Compare.newBuilder()
                    .setResult(CompareResult.EQUAL)
                    .setTarget(CompareTarget.MOD)
                    .setKey(getKey(key))
                    .setModRevision(revision))
                .addSuccess(RequestOp.newBuilder()
                    .setRequestPut(PutRequest.newBuilder()
                        .setHeader(HEADER)
                        .setKey(getKey(key))
                        .setValue(getValue(value))
                        .setPrevKv(true)
                        .build()))
                .addFailure(RequestOp.newBuilder()
                    .setRequestRange(RangeRequest.newBuilder()
                        .setHeader(HEADER)
                        .setKey(getKey(key))
                        .build()))
                .build()));
    }

    KeyValue deleteKeyFromTableStore(int key) throws Exception {
        DeleteRangeResponse response = result(
            tableStore.delete(DeleteRangeRequest.newBuilder()
                .setHeader(HEADER)
                .setKey(getKey(key))
                .setPrevKv(true)
                .build()));

        assertEquals(StatusCode.SUCCESS, response.getHeader().getCode());
        assertEquals(HEADER, response.getHeader().getRoutingHeader());
        if (0 == response.getPrevKvsCount()) {
            return null;
        } else {
            return response.getPrevKvs(0);
        }
    }

    List<KeyValue> deleteRange(int startKey, int endKey) throws Exception {
        DeleteRangeResponse delResp = result(
            tableStore.delete(DeleteRangeRequest.newBuilder()
                .setHeader(HEADER)
                .setKey(getKey(startKey))
                .setRangeEnd(getKey(endKey))
                .setPrevKv(true)
                .build()));

        assertEquals(StatusCode.SUCCESS, delResp.getHeader().getCode());
        assertEquals(HEADER, delResp.getHeader().getRoutingHeader());
        return delResp.getPrevKvsList();
    }

    List<KeyValue> range(int startKey, int endKey) throws Exception {
        RangeResponse rangeResp = result(
            tableStore.range(RangeRequest.newBuilder()
                .setHeader(HEADER)
                .setKey(getKey(startKey))
                .setRangeEnd(getKey(endKey))
                .build()));

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
            assertEquals(getKey(0), kv.getKey());
            assertEquals(getValue(0), kv.getValue());
        }

        // putIfAbsent
        {
            // failure case
            KeyValue prevKV = putIfAbsentToTableStore(0, 99, false);
            assertNotNull(prevKV);
            assertEquals(getKey(0), prevKV.getKey());
            assertEquals(getValue(0), prevKV.getValue());
            // get key(0)
            KeyValue kv = getKeyValue(0);
            assertEquals(getKey(0), kv.getKey());
            assertEquals(getValue(0), kv.getValue());
            // success case
            prevKV = putIfAbsentToTableStore(1, 1, true);
            assertNull(prevKV);
            // get key(1)
            kv = getKeyValue(1);
            assertEquals(getKey(1), kv.getKey());
            assertEquals(getValue(1), kv.getValue());
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
            assertEquals(getKey(key), prevKV.getKey());
            assertEquals(getValue(initialVal), prevKV.getValue());
            assertEquals(0, prevKV.getVersion());

            KeyValue kv = getKeyValue(key);
            assertEquals(getKey(key), kv.getKey());
            assertEquals(getValue(casVal), kv.getValue());
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
            assertEquals(getValue(initialVal), kv.getValue());

            // rPut(key2, v, 0)
            response = rPutToTableStore(key, casVal, revision);
            assertEquals(StatusCode.SUCCESS, response.getHeader().getCode());

            kv = getKeyValue(key);
            assertEquals(revision + 1, kv.getModRevision());
            assertEquals(getValue(casVal), kv.getValue());
        }

        // delete(k)
        {
            // key not found
            KeyValue kv = deleteKeyFromTableStore(99);
            assertNull(kv);
            // key exists
            int key = 0;
            kv = getKeyValue(key);
            assertEquals(getKey(key), kv.getKey());
            assertEquals(getValue(key), kv.getValue());
            kv = deleteKeyFromTableStore(key);
            assertNotNull(kv);
            assertEquals(getKey(key), kv.getKey());
            assertEquals(getValue(key), kv.getValue());
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
            assertEquals(getKey(idx), kv.getKey());
            assertEquals(getValue(idx), kv.getValue());
            // revision - starts from 1, but the first revision is used for nop barrier record.
            assertEquals(idx + startCreateRevision, kv.getCreateRevision());
            assertEquals(idx + startModRevision, kv.getModRevision());
            assertEquals(expectedVersion, kv.getVersion());
            ++idx;
        }
        assertEquals(endKey + 1, idx);
    }

}
