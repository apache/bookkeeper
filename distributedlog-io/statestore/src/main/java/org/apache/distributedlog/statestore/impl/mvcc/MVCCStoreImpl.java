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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.distributedlog.common.coder.Coder;
import org.apache.distributedlog.statestore.api.KV;
import org.apache.distributedlog.statestore.api.KVIterator;
import org.apache.distributedlog.statestore.api.KVMulti;
import org.apache.distributedlog.statestore.api.KVStore;
import org.apache.distributedlog.statestore.api.StateStoreSpec;
import org.apache.distributedlog.statestore.api.mvcc.KVRecord;
import org.apache.distributedlog.statestore.api.mvcc.MVCCStore;
import org.apache.distributedlog.statestore.api.mvcc.op.DeleteOp;
import org.apache.distributedlog.statestore.api.mvcc.op.PutOp;
import org.apache.distributedlog.statestore.api.mvcc.op.RangeOp;
import org.apache.distributedlog.statestore.api.mvcc.op.TxnOp;
import org.apache.distributedlog.statestore.api.mvcc.result.Code;
import org.apache.distributedlog.statestore.api.mvcc.result.DeleteResult;
import org.apache.distributedlog.statestore.api.mvcc.result.PutResult;
import org.apache.distributedlog.statestore.api.mvcc.result.RangeResult;
import org.apache.distributedlog.statestore.api.mvcc.result.TxnResult;
import org.apache.distributedlog.statestore.exceptions.StateStoreException;
import org.apache.distributedlog.statestore.impl.mvcc.op.RangeOpImpl;
import org.apache.distributedlog.statestore.impl.mvcc.result.PutResultImpl;
import org.apache.distributedlog.statestore.impl.mvcc.result.RangeResultImpl;
import org.apache.distributedlog.statestore.impl.mvcc.result.ResultFactory;

/**
 * MVCC Store Implementation.
 */
class MVCCStoreImpl<K, V> implements MVCCStore<K, V> {

    private final ResultFactory<K, V> resultFactory;
    private final KVRecordFactory<K, V> recordFactory;

    Coder<K> keyCoder;
    Coder<V> valCoder;
    // use raw bytes since rocksdb support bytes now.
    KVStore<byte[], MVCCRecord> kvStore;

    @Override
    public String name() {
        return null;
    }

    @Override
    public void init(StateStoreSpec spec) throws StateStoreException {

    }

    @Override
    public void flush() throws StateStoreException {

    }

    @Override
    public void close() {

    }

    //
    // Write View
    //

    @Override
    public PutResult<K, V> put(PutOp<K, V> op) {
        return put(null, op);
    }

    private PutResult<K, V> put(@Nullable KVMulti<byte[], MVCCRecord> multi, PutOp<K, V> op) {
        // parameters
        final K key = op.key();
        final V val = op.value();
        final long revision = op.revision();

        // result
        final PutResultImpl<K, V> result = resultFactory.newPutResult();

        // raw key & value
        final byte[] rawKey = keyCoder.encode(key);
        final ByteBuf rawValBuf = valCoder.encodeBuf(val);

        MVCCRecord record = kvStore.get(rawKey);
        MVCCRecord oldRecord = null;
        try {
            if (null != record) {
                // validate the update revision before applying the update to the record
                if (record.compareModRev(revision) >= 0) {
                    result.setCode(Code.SMALLER_REVISION);
                    return result;
                }

                if (op.prevKV()) {
                    // make a copy before modification
                    oldRecord = record.duplicate();
                }
                record.setVersion(record.getVersion() + 1);
            } else {
                record = MVCCRecord.newRecord();
                record.setCreateRev(revision);
                record.setVersion(0);
            }
            record.setValue(rawValBuf);
            record.setModRev(revision);

            // write the mvcc record back
            if (null != multi) {
                multi.put(rawKey, record);
            } else {
                kvStore.put(rawKey, record);
            }

            // finalize the result
            result.setCode(Code.OK);
            if (null == multi && null != oldRecord) {
                KVRecordImpl<K, V> prevKV = oldRecord.asKVRecord(
                    recordFactory,
                    key,
                    valCoder);
                result.setPrevKV(prevKV);
            }
            return result;
        } finally {
            record.recycle();
            if (null != oldRecord) {
                oldRecord.recycle();
            }
        }
    }

    @Override
    public DeleteResult<K, V> delete(DeleteOp<K, V> op) {
        return null;
    }

    @Override
    public TxnResult<K, V> txn(TxnOp<K, V> op) {
        return null;
    }

    //
    // Read View
    //

    private boolean getKeyRecords(byte[] rawKey,
                                  @Nullable byte[] rawEndKey,
                                  List<byte[]> resultKeys,
                                  List<MVCCRecord> resultValues,
                                  MutableLong numKvs,
                                  @Nullable Predicate<MVCCRecord> predicate,
                                  int limit,
                                  boolean countOnly) {
        try (KVIterator<byte[], MVCCRecord> iter = kvStore.range(rawKey, rawEndKey)) {
            while (iter.hasNext() && (limit < 0 || resultKeys.size() < limit)) {
                KV<byte[], MVCCRecord> kv = iter.next();
                byte[] key = kv.key();
                MVCCRecord val = kv.value();

                processRecord(key, val, resultKeys, resultValues, numKvs, predicate, countOnly);
            }
            return iter.hasNext();
        }
    }

    private void processRecord(byte[] key,
                               MVCCRecord record,
                               List<byte[]> resultKeys,
                               List<MVCCRecord> resultValues,
                               MutableLong numKvs,
                               @Nullable Predicate<MVCCRecord> predicate,
                               boolean countOnly) {
        if (null == predicate && countOnly) {
            numKvs.increment();
            return;
        }

        if (null == predicate || predicate.test(record)) {
            numKvs.increment();
            if (countOnly) {
                record.recycle();
            } else {
                resultKeys.add(key);
                resultValues.add(record);
            }
        } else {
            record.recycle();
        }
    }

    @Override
    public RangeResult<K, V> range(RangeOp<K, V> rangeOp) {
        // parameters
        final K key = rangeOp.key();
        final K endKey = rangeOp.endKey();
        final RangeOpImpl<K, V> rangeOpImpl = (RangeOpImpl<K, V>) rangeOp;

        // result
        final RangeResultImpl<K, V> result = resultFactory.newRangeResult();

        // raw key
        final byte[] rawKey = keyCoder.encode(key);

        if (null == endKey) {
            // point lookup
            MVCCRecord record = kvStore.get(rawKey);
            try {
                if (null == record || !rangeOpImpl.test(record)) {
                    result.setCount(0);
                    result.setKvs(Collections.emptyList());
                } else {
                    result.setCount(1);
                    result.setKvs(Lists.newArrayList(record.asKVRecord(
                        recordFactory,
                        key,
                        valCoder)));
                }
                result.setHasMore(false);
                result.setCode(Code.OK);
                return result;
            } finally {
                if (null != record) {
                    record.recycle();
                }
            }
        }

        // range lookup
        byte[] rawEndKey = keyCoder.encode(endKey);
        List<byte[]> keys = Lists.newArrayList();
        List<MVCCRecord> records = Lists.newArrayList();
        MutableLong numKvs = new MutableLong(0L);

        try {

            boolean hasMore = getKeyRecords(
                rawKey,
                rawEndKey,
                keys,
                records,
                numKvs,
                rangeOpImpl,
                rangeOp.limit(),
                false);

            List<KVRecord<K, V>> kvs = Lists.newArrayListWithExpectedSize(keys.size());

            for (int i = 0; i < keys.size(); i++) {
                byte[] keyBytes = keys.get(i);
                MVCCRecord record = records.get(i);
                kvs.add(record.asKVRecord(
                    recordFactory,
                    keyCoder.decode(keyBytes),
                    valCoder
                ));
            }

            result.setCode(Code.OK);
            result.setKvs(kvs);
            result.setCount(kvs.size());
            result.setHasMore(hasMore);

        } finally {
            records.forEach(MVCCRecord::recycle);
        }
        return result;
    }
}
