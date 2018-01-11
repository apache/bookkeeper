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

import static org.apache.distributedlog.statestore.impl.Constants.NULL_END_KEY;
import static org.apache.distributedlog.statestore.impl.Constants.NULL_START_KEY;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.SignedBytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.api.statestore.exceptions.InvalidStateStoreException;
import org.apache.distributedlog.api.statestore.exceptions.MVCCStoreException;
import org.apache.distributedlog.api.statestore.exceptions.StateStoreRuntimeException;
import org.apache.distributedlog.api.statestore.kv.KV;
import org.apache.distributedlog.api.statestore.kv.KVIterator;
import org.apache.distributedlog.api.statestore.kv.KVMulti;
import org.apache.distributedlog.api.statestore.mvcc.KVRecord;
import org.apache.distributedlog.api.statestore.mvcc.MVCCStore;
import org.apache.distributedlog.api.statestore.mvcc.op.CompareOp;
import org.apache.distributedlog.api.statestore.mvcc.op.CompareResult;
import org.apache.distributedlog.api.statestore.mvcc.op.CompareTarget;
import org.apache.distributedlog.api.statestore.mvcc.op.DeleteOp;
import org.apache.distributedlog.api.statestore.mvcc.op.Op;
import org.apache.distributedlog.api.statestore.mvcc.op.OpFactory;
import org.apache.distributedlog.api.statestore.mvcc.op.PutOp;
import org.apache.distributedlog.api.statestore.mvcc.op.RangeOp;
import org.apache.distributedlog.api.statestore.mvcc.op.TxnOp;
import org.apache.distributedlog.api.statestore.mvcc.result.Code;
import org.apache.distributedlog.api.statestore.mvcc.result.DeleteResult;
import org.apache.distributedlog.api.statestore.mvcc.result.PutResult;
import org.apache.distributedlog.api.statestore.mvcc.result.RangeResult;
import org.apache.distributedlog.api.statestore.mvcc.result.Result;
import org.apache.distributedlog.api.statestore.mvcc.result.TxnResult;
import org.apache.distributedlog.common.coder.Coder;
import org.apache.distributedlog.statestore.impl.Constants;
import org.apache.distributedlog.statestore.impl.kv.KVImpl;
import org.apache.distributedlog.statestore.impl.kv.RocksdbKVStore;
import org.apache.distributedlog.statestore.impl.mvcc.op.OpFactoryImpl;
import org.apache.distributedlog.statestore.impl.mvcc.op.RangeOpImpl;
import org.apache.distributedlog.statestore.impl.mvcc.result.DeleteResultImpl;
import org.apache.distributedlog.statestore.impl.mvcc.result.PutResultImpl;
import org.apache.distributedlog.statestore.impl.mvcc.result.RangeResultImpl;
import org.apache.distributedlog.statestore.impl.mvcc.result.ResultFactory;
import org.apache.distributedlog.statestore.impl.mvcc.result.TxnResultImpl;
import org.apache.distributedlog.statestore.impl.rocksdb.RocksUtils;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

/**
 * MVCC Store Implementation.
 *
 * <p>The current implementation executes write operations in one single io thread.
 * It can be improved later to leverage the revision numbers to achieve mvcc.
 */
@Slf4j
class MVCCStoreImpl<K, V> extends RocksdbKVStore<K, V> implements MVCCStore<K, V> {

    private static final Comparator<byte[]> COMPARATOR = SignedBytes.lexicographicalComparator();

    private final ResultFactory<K, V> resultFactory;
    private final KVRecordFactory<K, V> recordFactory;
    private final OpFactory<K, V> opFactory;
    private final Coder<MVCCRecord> recordCoder = MVCCRecordCoder.of();

    MVCCStoreImpl() {
        this.resultFactory = new ResultFactory<>();
        this.recordFactory = new KVRecordFactory<>();
        this.opFactory = new OpFactoryImpl<>();
    }

    @Override
    public OpFactory<K, V> getOpFactory() {
        return opFactory;
    }

    @Override
    public void put(K key, V value) {
        throw new UnsupportedOperationException("Please use #put(PutOp op) instead");
    }

    @Override
    public synchronized V putIfAbsent(K key, V value) {
        throw new UnsupportedOperationException("Please use #put(PutOp op) instead");
    }

    @Override
    public synchronized KVMulti<K, V> multi() {
        throw new UnsupportedOperationException("Please use #txn(TxnOp op) instead");
    }

    @Override
    public synchronized V delete(K key) {
        throw new UnsupportedOperationException("Please use #delete(DeleteOp op) instead");
    }

    void put(K key, V value, long revision) {
        PutOp<K, V> op = opFactory.buildPutOp()
            .key(key)
            .value(value)
            .prevKV(false)
            .revision(revision)
            .build();
        PutResult<K, V> result = null;
        try {
            result = put(op);
            if (Code.OK != result.code()) {
                throw new MVCCStoreException(result.code(),
                    "Failed to put (" + key + ", " + value + ", " + revision + ") to state store " + name);
            }
        } finally {
            if (null != result) {
                result.recycle();
            }
        }
    }

    void delete(K key, long revision) {
        DeleteOp<K, V> op = opFactory.buildDeleteOp()
            .nullableKey(key)
            .prevKV(false)
            .revision(revision)
            .build();
        DeleteResult<K, V> result = null;
        try {
            result = delete(op);
            if (Code.OK != result.code()) {
                throw new MVCCStoreException(result.code(),
                    "Failed to delete key=" + key + "from state store " + name);
            }
        } finally {
            if (null != result) {
                result.recycle();
            }
        }

    }

    void deleteRange(K key, K endKey, long revision) {
        DeleteOp<K, V> op = opFactory.buildDeleteOp()
            .nullableKey(key)
            .nullableEndKey(endKey)
            .prevKV(false)
            .revision(revision)
            .isRangeOp(true)
            .build();
        DeleteResult<K, V> result = null;
        try {
            result = delete(op);
            if (Code.OK != result.code()) {
                throw new MVCCStoreException(result.code(),
                    "Failed to delete key=" + key + "from state store " + name);
            }
        } finally {
            if (null != result) {
                result.recycle();
            }
        }

    }

    @Override
    public synchronized V get(K key) {
        RangeOp<K, V> op = opFactory.buildRangeOp()
            .nullableKey(key)
            .limit(1)
            .build();
        RangeResult<K, V> result = null;
        try {
            result = range(op);
            if (Code.OK != result.code()) {
                throw new MVCCStoreException(result.code(),
                    "Failed to retrieve key from store " + name + " : code = " + result.code());
            }
            if (result.count() <= 0) {
                return null;
            } else {
                return result.kvs().get(0).value();
            }
        } finally {
            if (null != result) {
                result.recycle();
            }
        }
    }

    @Override
    public synchronized KVIterator<K, V> range(K from, K to) {
        checkStoreOpen();

        RangeResultIterator iter = new RangeResultIterator(from, to);
        kvIters.add(iter);
        return iter;
    }

    class RangeResultIterator implements KVIterator<K, V> {

        private final K to;
        private K next;
        private RangeResult<K, V> result;
        private PeekingIterator<KVRecord<K, V>> resultIter;
        private boolean eor = false;

        private volatile boolean closed = false;

        RangeResultIterator(K from, K to) {
            this.to = to;
            this.next = from;
        }

        private void ensureIteratorOpen() {
            if (closed) {
                throw new InvalidStateStoreException("MVCC state store " + name + " is already closed.");
            }
        }

        @Override
        public void close() {
            kvIters.remove(this);
            if (null != result) {
                result.recycle();
            }
            closed = true;
        }

        private void getNextBatch() {
            RangeOp<K, V> op = opFactory.buildRangeOp()
                .nullableKey(next)
                .nullableEndKey(to)
                .isRangeOp(true)
                .limit(32)
                .build();
            this.result = range(op);
            if (Code.OK != result.code()) {
                throw new MVCCStoreException(result.code(),
                    "Failed to fetch kv pairs at range [" + next + ", " + to + "] from state store " + name);
            }
            this.resultIter = Iterators.peekingIterator(result.kvs().iterator());
        }

        private void skipFirstKey() {
            while (this.resultIter.hasNext()) {
                KVRecord<K, V> kv = this.resultIter.peek();
                if (!kv.key().equals(next)) {
                    break;
                }
                this.resultIter.next();
            }
        }

        @Override
        public boolean hasNext() {
            ensureIteratorOpen();

            if (eor) {
                return false;
            }
            if (null == result) {
                getNextBatch();
            }
            if (!this.resultIter.hasNext()) {
                if (this.result.hasMore()) {
                    this.result.recycle();
                    getNextBatch();
                    skipFirstKey();
                    return hasNext();
                } else {
                    eor = true;
                    return false;
                }
            }
            return true;
        }

        @Override
        public KV<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            KVRecord<K, V> kv = this.resultIter.next();
            next = kv.key();
            if (next.equals(to)) {
                eor = true;
            }
            return new KVImpl<>(kv.key(), kv.value());
        }
    }

    //
    // Write View
    //

    private void executeBatch(WriteBatch batch) {
        try {
            db.write(writeOpts, batch);
        } catch (RocksDBException e) {
            throw new StateStoreRuntimeException("Error while executing a multi operation from state store " + name, e);
        }
    }

    @Override
    public PutResult<K, V> put(PutOp<K, V> op) {
        return put(op.revision(), op);
    }

    PutResult<K, V> put(long revision, PutOp<K, V> op) {
        try {
            return processPut(revision, op);
        } catch (MVCCStoreException e) {
            PutResultImpl<K, V> result = resultFactory.newPutResult(revision);
            result.setCode(e.getCode());
            return result;
        } catch (StateStoreRuntimeException e) {
            PutResultImpl<K, V> result = resultFactory.newPutResult(revision);
            result.setCode(Code.INTERNAL_ERROR);
            return result;
        }
    }

    synchronized PutResult<K, V> processPut(long revision, PutOp<K, V> op) {
        checkStoreOpen();

        WriteBatch batch = new WriteBatch();
        PutResult<K, V> result = null;
        try {
            result = put(revision, batch, op);
            executeBatch(batch);
            return result;
        } catch (StateStoreRuntimeException e) {
            if (null != result) {
                result.recycle();
            }
            throw e;
        } finally {
            RocksUtils.close(batch);
        }
    }

    private PutResult<K, V> put(long revision, WriteBatch batch, PutOp<K, V> op) {
        // parameters
        final K key = op.key();
        final V val = op.value();

        // raw key & value
        final byte[] rawKey = keyCoder.encode(key);
        final ByteBuf rawValBuf = valCoder.encodeBuf(val);

        MVCCRecord record;
        try {
            record = getKeyRecord(key, rawKey);
        } catch (StateStoreRuntimeException e) {
            rawValBuf.release();
            throw e;
        }

        // result
        final PutResultImpl<K, V> result = resultFactory.newPutResult(revision);
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
            batch.put(dataCfHandle, rawKey, recordCoder.encode(record));

            // finalize the result
            result.setCode(Code.OK);
            if (null != oldRecord) {
                KVRecordImpl<K, V> prevKV = oldRecord.asKVRecord(
                    recordFactory,
                    key,
                    valCoder);
                result.setPrevKV(prevKV);
            }
            return result;
        } catch (StateStoreRuntimeException e) {
            result.recycle();
            throw e;
        } finally {
            if (null != record) {
                record.recycle();
            }
            if (null != oldRecord) {
                oldRecord.recycle();
            }
        }
    }

    //
    // Delete Op
    //

    @Override
    public DeleteResult<K, V> delete(DeleteOp<K, V> op) {
        return delete(op.revision(), op);
    }

    DeleteResult<K, V> delete(long revision, DeleteOp<K, V> op) {
        try {
            return processDelete(revision, op);
        } catch (MVCCStoreException e) {
            DeleteResultImpl<K, V> result = resultFactory.newDeleteResult(revision);
            result.setCode(e.getCode());
            return result;
        } catch (StateStoreRuntimeException e) {
            DeleteResultImpl<K, V> result = resultFactory.newDeleteResult(revision);
            result.setCode(Code.INTERNAL_ERROR);
            return result;
        }
    }

    synchronized DeleteResult<K, V> processDelete(long revision, DeleteOp<K, V> op) {
        checkStoreOpen();

        WriteBatch batch = new WriteBatch();
        DeleteResult<K, V> result = null;
        try {
            result = delete(revision, batch, op, true);
            executeBatch(batch);
            return result;
        } catch (StateStoreRuntimeException e) {
            if (null != result) {
                result.recycle();
            }
            throw e;
        } finally {
            RocksUtils.close(batch);
        }
    }

    DeleteResult<K, V> delete(long revision, WriteBatch batch, DeleteOp<K, V> op, boolean allowBlind) {
        // parameters
        final K key = op.key().orElse(null);
        final K endKey = op.endKey().orElse(null);
        final boolean blind = allowBlind && !op.prevKV();

        final byte[] rawKey = (null != key) ? keyCoder.encode(key) : NULL_START_KEY;
        final byte[] rawEndKey = (null != endKey) ? keyCoder.encode(endKey) : (op.isRangeOp() ? NULL_END_KEY : null);

        // result
        final DeleteResultImpl<K, V> result = resultFactory.newDeleteResult(revision);
        final List<byte[]> keys = Lists.newArrayList();
        final List<MVCCRecord> records = Lists.newArrayList();
        try {
            long numDeleted;
            if (blind) {
                deleteBlind(batch, rawKey, rawEndKey);
                numDeleted = 0;
            } else {
                numDeleted = deleteUsingIter(
                    batch,
                    key,
                    rawKey,
                    rawEndKey,
                    keys,
                    records,
                    false);
            }

            List<KVRecord<K, V>> kvs = toKvs(keys, records);

            result.setCode(Code.OK);
            result.setPrevKvs(kvs);
            result.setNumDeleted(numDeleted);
        } catch (StateStoreRuntimeException e) {
            result.recycle();
            throw e;
        } finally {
            records.forEach(MVCCRecord::recycle);
        }
        return result;
    }

    /**
     * Delete blind should be call as the last op in the delete operations.
     * Since we need to modify endKey to make {@link WriteBatch#deleteRange(byte[], byte[])}
     * delete the end key.
     */
    void deleteBlind(WriteBatch batch,
                     byte[] key,
                     @Nullable byte[] endKey) {
        if (null == endKey) {
            batch.remove(key);
        } else {
            Pair<byte[], byte[]> realRange = getRealRange(key, endKey);
            endKey = realRange.getRight();
            ++endKey[endKey.length - 1];
            batch.deleteRange(realRange.getLeft(), endKey);
        }
    }

    long deleteUsingIter(WriteBatch batch,
                         K key,
                         byte[] rawKey,
                         @Nullable byte[] rawEndKey,
                         List<byte[]> resultKeys,
                         List<MVCCRecord> resultValues,
                         boolean countOnly) {
        MutableLong numKvs = new MutableLong(0L);
        if (null == rawEndKey) {
            MVCCRecord record = getKeyRecord(key, rawKey);
            if (null != record) {
                if (!countOnly) {
                    resultKeys.add(rawKey);
                    resultValues.add(record);
                } else {
                    record.recycle();
                }
                numKvs.add(1L);
                batch.remove(rawKey);
            }
        } else {
            Pair<byte[], byte[]> realRange = getRealRange(rawKey, rawEndKey);
            rawKey = realRange.getLeft();
            rawEndKey = realRange.getRight();

            getKeyRecords(
                rawKey,
                rawEndKey,
                resultKeys,
                resultValues,
                numKvs,
                record -> true,
                -1,
                countOnly);

            deleteBlind(batch, rawKey, rawEndKey);
        }
        return numKvs.longValue();
    }

    //
    // Txn Op
    //

    @Override
    public TxnResult<K, V> txn(TxnOp<K, V> op) {
        return txn(op.revision(), op);
    }

    synchronized TxnResult<K, V> txn(long revision, TxnOp<K, V> op) {
        try {
            return processTxn(revision, op);
        } catch (MVCCStoreException e) {
            TxnResultImpl<K, V> result = resultFactory.newTxnResult(revision);
            result.setCode(e.getCode());
            return result;
        } catch (StateStoreRuntimeException e) {
            TxnResultImpl<K, V> result = resultFactory.newTxnResult(revision);
            result.setCode(Code.INTERNAL_ERROR);
            return result;
        }
    }

    synchronized TxnResult<K, V> processTxn(long revision, TxnOp<K, V> op) {
        checkStoreOpen();

        // 1. process the compares
        boolean success = processCompares(op);

        // 2. prepare the response list
        List<Op<K, V>> operations;
        List<Result<K, V>> results;
        if (success) {
            operations = op.successOps();
        } else {
            operations = op.failureOps();
        }
        results = Lists.newArrayListWithExpectedSize(operations.size());

        // 3. process the operations
        try (WriteBatch batch = new WriteBatch()) {
            for (Op o : operations) {
                results.add(executeOp(revision, batch, o));
            }
            executeBatch(batch);

            // 4. repare the result
            TxnResultImpl<K, V> txnResult = resultFactory.newTxnResult(revision);
            txnResult.setSuccess(success);
            txnResult.setResults(results);
            txnResult.setCode(Code.OK);

            return txnResult;
        } catch (StateStoreRuntimeException e) {
            results.forEach(Result::recycle);
            throw e;
        }

    }

    boolean processCompareOp(CompareOp<K, V> op) {
        MVCCRecord record = null;
        K key = op.getKey();
        byte[] rawKey = keyCoder.encode(key);
        try {
            record = getKeyRecord(key, rawKey);
            if (null == record) {
                if (CompareTarget.VALUE != op.getTarget()) {
                    throw new MVCCStoreException(Code.KEY_NOT_FOUND, "Key " + key + " is not found");
                }
            }
            return processCompareOp(record, op);
        } finally {
            if (null != record) {
                record.recycle();
            }
        }
    }

    boolean processCompareOp(@Nullable MVCCRecord record,
                             CompareOp<K, V> op) {
        int cmp;
        switch (op.getTarget()) {
            case MOD:
                cmp = record.compareModRev(op.getRevision());
                break;
            case CREATE:
                cmp = record.compareCreateRev(op.getRevision());
                break;
            case VERSION:
                cmp = record.compareVersion(op.getRevision());
                break;
            case VALUE:
                if (null == record) { // key not found
                    if (CompareResult.EQUAL == op.getResult()) {
                        return !op.getValue().isPresent();
                    } else if (CompareResult.NOT_EQUAL == op.getResult()) {
                        return op.getValue().isPresent();
                    } else {
                        return false;
                    }
                }
                // key is found and value-to-compare is present
                if (op.getValue().isPresent()) {
                    byte[] rawValue = valCoder.encode(op.getValue().get());
                    cmp = record.getValue().compareTo(Unpooled.wrappedBuffer(rawValue));
                } else {
                    // key is found but value-to-compare is missing
                    switch (op.getResult()) {
                        case EQUAL:
                        case LESS:
                            return false;
                        default:
                            return true;
                    }
                }
                break;
            default:
                return false;
        }
        boolean success;
        switch (op.getResult()) {
            case LESS:
                success = cmp < 0;
                break;
            case EQUAL:
                success = cmp == 0;
                break;
            case GREATER:
                success = cmp > 0;
                break;
            case NOT_EQUAL:
                success = cmp != 0;
                break;
            default:
                success = false;
                break;
        }
        return success;
    }

    boolean processCompares(TxnOp<K, V> op) {
        for (CompareOp<K, V> compare : op.compareOps()) {
            if (processCompareOp(compare)) {
                continue;
            }
            return false;
        }
        return true;
    }

    private Result<K, V> executeOp(long revision, WriteBatch batch, Op<K, V> op) {
        if (op instanceof PutOp) {
            return put(revision, batch, (PutOp<K, V>) op);
        } else if (op instanceof DeleteOp) {
            return delete(revision, batch, (DeleteOp<K, V>) op, true);
        } else if (op instanceof RangeOp){
            return range((RangeOp<K, V>) op);
        } else {
            throw new MVCCStoreException(Code.ILLEGAL_OP, "Unknown operation in a transaction : " + op);
        }
    }

    //
    // Read View
    //

    private boolean getKeyRecords(byte[] rawKey,
                                  byte[] rawEndKey,
                                  List<byte[]> resultKeys,
                                  List<MVCCRecord> resultValues,
                                  MutableLong numKvs,
                                  @Nullable Predicate<MVCCRecord> predicate,
                                  long limit,
                                  boolean countOnly) {
        try (RocksIterator iter = db.newIterator(dataCfHandle)) {
            iter.seek(rawKey);
            boolean eor = false;
            while (iter.isValid() && (limit < 0 || resultKeys.size() < limit)) {
                byte[] key = iter.key();
                if (COMPARATOR.compare(rawEndKey, key) < 0) {
                    eor = true;
                    break;
                }
                MVCCRecord val = recordCoder.decode(iter.value());

                processRecord(key, val, resultKeys, resultValues, numKvs, predicate, countOnly);

                iter.next();
            }
            if (eor) {
                return false;
            } else {
                return iter.isValid();
            }
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

    private MVCCRecord getKeyRecord(K key, byte[] keyBytes) {
        try {
            byte[] valBytes = this.db.get(dataCfHandle, keyBytes);
            if (null == valBytes) {
                return null;
            }
            return recordCoder.decode(valBytes);
        } catch (RocksDBException e) {
            throw new StateStoreRuntimeException("Error while getting value for key "
                + key + " from state store " + name, e);
        }

    }

    @Override
    public RangeResult<K, V> range(RangeOp<K, V> rangeOp) {
        try {
            return processRange(rangeOp);
        } catch (MVCCStoreException e) {
            RangeResultImpl<K, V> result = resultFactory.newRangeResult(rangeOp.revision());
            result.setCode(e.getCode());
            return result;
        } catch (StateStoreRuntimeException e) {
            RangeResultImpl<K, V> result = resultFactory.newRangeResult(rangeOp.revision());
            result.setCode(Code.INTERNAL_ERROR);
            return result;
        }
    }

    synchronized RangeResult<K, V> processRange(RangeOp<K, V> rangeOp) {
        checkStoreOpen();

        // parameters
        final K key = rangeOp.key().orElse(null);
        final K endKey = rangeOp.endKey().orElse(null);
        final RangeOpImpl<K, V> rangeOpImpl = (RangeOpImpl<K, V>) rangeOp;

        // result
        final RangeResultImpl<K, V> result = resultFactory.newRangeResult(rangeOp.revision());

        // raw key
        byte[] rawKey = (null != key) ? keyCoder.encode(key) : NULL_START_KEY;

        if (null == endKey && !rangeOp.isRangeOp()) {
            // point lookup
            MVCCRecord record = getKeyRecord(key, rawKey);
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
        byte[] rawEndKey = (null != endKey) ? keyCoder.encode(endKey) : NULL_END_KEY;
        Pair<byte[], byte[]> realRange = getRealRange(rawKey, rawEndKey);
        rawKey = realRange.getLeft();
        rawEndKey = realRange.getRight();

        // range lookup
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

            List<KVRecord<K, V>> kvs = toKvs(keys, records);

            result.setCode(Code.OK);
            result.setKvs(kvs);
            result.setCount(kvs.size());
            result.setHasMore(hasMore);
        } finally {
            records.forEach(MVCCRecord::recycle);
        }
        return result;
    }

    private List<KVRecord<K, V>> toKvs(List<byte[]> keys, List<MVCCRecord> records) {
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
        return kvs;
    }

    private Pair<byte[], byte[]> getRealRange(byte[] rawKey, byte[] rawEndKey) {
        boolean isNullStartKey = Constants.isNullStartKey(rawKey);
        boolean isNullEndKey = Constants.isNullEndKey(rawEndKey);
        if (isNullStartKey || isNullEndKey) {
            try (RocksIterator iter = db.newIterator(dataCfHandle)) {
                if (isNullStartKey) {
                    iter.seekToFirst();
                    if (!iter.isValid()) {
                        // no key to delete
                        return null;
                    }
                    rawKey = iter.key();
                }
                if (isNullEndKey) {
                    iter.seekToLast();
                    if (!iter.isValid()) {
                        // no key to delete
                        return null;
                    }
                    rawEndKey = iter.key();
                }
            }
        }
        return Pair.of(rawKey, rawEndKey);
    }
}
