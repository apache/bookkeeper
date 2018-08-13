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

import static io.netty.util.ReferenceCountUtil.retain;
import static org.apache.bookkeeper.statelib.impl.Constants.NULL_END_KEY;
import static org.apache.bookkeeper.statelib.impl.Constants.NULL_START_KEY;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.TextFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.impl.op.OpFactoryImpl;
import org.apache.bookkeeper.api.kv.impl.result.DeleteResultImpl;
import org.apache.bookkeeper.api.kv.impl.result.IncrementResultImpl;
import org.apache.bookkeeper.api.kv.impl.result.KeyValueFactory;
import org.apache.bookkeeper.api.kv.impl.result.KeyValueImpl;
import org.apache.bookkeeper.api.kv.impl.result.PutResultImpl;
import org.apache.bookkeeper.api.kv.impl.result.RangeResultImpl;
import org.apache.bookkeeper.api.kv.impl.result.ResultFactory;
import org.apache.bookkeeper.api.kv.impl.result.TxnResultImpl;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.op.CompareTarget;
import org.apache.bookkeeper.api.kv.op.DeleteOp;
import org.apache.bookkeeper.api.kv.op.IncrementOp;
import org.apache.bookkeeper.api.kv.op.Op;
import org.apache.bookkeeper.api.kv.op.OpFactory;
import org.apache.bookkeeper.api.kv.op.PutOp;
import org.apache.bookkeeper.api.kv.op.RangeOp;
import org.apache.bookkeeper.api.kv.op.TxnOp;
import org.apache.bookkeeper.api.kv.options.Options;
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.apache.bookkeeper.api.kv.result.Code;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.api.kv.result.IncrementResult;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.api.kv.result.PutResult;
import org.apache.bookkeeper.api.kv.result.RangeResult;
import org.apache.bookkeeper.api.kv.result.Result;
import org.apache.bookkeeper.api.kv.result.TxnResult;
import org.apache.bookkeeper.common.coder.Coder;
import org.apache.bookkeeper.common.kv.KV;
import org.apache.bookkeeper.common.kv.KVImpl;
import org.apache.bookkeeper.statelib.api.exceptions.InvalidStateStoreException;
import org.apache.bookkeeper.statelib.api.exceptions.MVCCStoreException;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreRuntimeException;
import org.apache.bookkeeper.statelib.api.kv.KVIterator;
import org.apache.bookkeeper.statelib.api.kv.KVMulti;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCStore;
import org.apache.bookkeeper.statelib.impl.Constants;
import org.apache.bookkeeper.statelib.impl.kv.RocksdbKVStore;
import org.apache.bookkeeper.statelib.impl.rocksdb.RocksUtils;
import org.apache.bookkeeper.stream.proto.kv.store.ValueType;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
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

    private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();

    private final ResultFactory<K, V> resultFactory;
    private final KeyValueFactory<K, V> recordFactory;
    private final OpFactory<K, V> opFactory;
    private final Coder<MVCCRecord> recordCoder = MVCCRecordCoder.of();

    MVCCStoreImpl() {
        this.resultFactory = new ResultFactory<>();
        this.recordFactory = new KeyValueFactory<>();
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

    void increment(K key, long amount, long revision) {
        try (IncrementOp<K, V> op = opFactory.newIncrement(key, amount, Options.blindIncrement())) {
            try (IncrementResult<K, V> result = increment(revision, op)) {
                if (Code.OK != result.code()) {
                    throw new MVCCStoreException(result.code(),
                        "Failed to increment (" + key + ", " + amount + ") to state store " + name);
                }
            }
        }
    }

    void put(K key, V value, long revision) {
        try (PutOp<K, V> op = opFactory.newPut(
            key, value,
            Options.blindPut())) {
            try (PutResult<K, V> result = put(revision, op)) {
                if (Code.OK != result.code()) {
                    throw new MVCCStoreException(result.code(),
                        "Failed to put (" + key + ", " + value + ", " + revision + ") to state store " + name);
                }
            }
        }
    }

    void delete(K key, long revision) {
        try (DeleteOp<K, V> op = opFactory.newDelete(
            key,
            Options.delete())) {
            try (DeleteResult<K, V> result = delete(revision, op)) {
                if (Code.OK != result.code()) {
                    throw new MVCCStoreException(result.code(),
                        "Failed to delete key=" + key + "from state store " + name);
                }
            }
        }
    }

    void deleteRange(K key, K endKey, long revision) {
        try (DeleteOp<K, V> op = opFactory.newDelete(
            key,
            opFactory.optionFactory().newDeleteOption()
                .endKey(endKey)
                .prevKv(false)
                .build())) {
            try (DeleteResult<K, V> result = delete(revision, op)) {
                if (Code.OK != result.code()) {
                    throw new MVCCStoreException(result.code(),
                        "Failed to delete key=" + key + "from state store " + name);
                }
            }
        }
    }

    Long getNumber(K key) {
        try (RangeOp<K, V> op = opFactory.newRange(
            key,
            opFactory.optionFactory().newRangeOption()
                .limit(1)
                .build())) {
            try (RangeResult<K, V> result = range(op)) {
                if (Code.OK != result.code()) {
                    throw new MVCCStoreException(result.code(),
                        "Failed to retrieve key from store " + name + " : code = " + result.code());
                }
                if (result.count() <= 0) {
                    return null;
                } else {
                    return result.kvs().get(0).numberValue();
                }
            }
        }
    }

    @Override
    public synchronized V get(K key) {
        try (RangeOp<K, V> op = opFactory.newRange(
            key,
            opFactory.optionFactory().newRangeOption()
                .limit(1)
                .build())) {
            try (RangeResult<K, V> result = range(op)) {
                if (Code.OK != result.code()) {
                    throw new MVCCStoreException(result.code(),
                        "Failed to retrieve key from store " + name + " : code = " + result.code());
                }
                if (result.count() <= 0) {
                    return null;
                } else {
                    return retain(result.kvs().get(0).value());
                }
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
        private PeekingIterator<KeyValue<K, V>> resultIter;
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
                result.close();
            }
            closed = true;
        }

        private void getNextBatch() {
            try (RangeOp<K, V> op = opFactory.newRange(
                next,
                opFactory.optionFactory().newRangeOption()
                    .endKey(to)
                    .limit(32)
                    .build())) {
                this.result = range(op);
            }
            if (Code.OK != result.code()) {
                throw new MVCCStoreException(result.code(),
                    "Failed to fetch kv pairs at range [" + next + ", " + to + "] from state store " + name);
            }
            this.resultIter = Iterators.peekingIterator(result.kvs().iterator());
        }

        private void skipFirstKey() {
            while (this.resultIter.hasNext()) {
                KeyValue<K, V> kv = this.resultIter.peek();
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
                if (this.result.more()) {
                    this.result.close();
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
            KeyValue<K, V> kv = this.resultIter.next();
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

    /**
     * TODO: the increment operation can be optimized using rocksdb merge operator.
     */
    @Override
    public IncrementResult<K, V> increment(long revision, IncrementOp<K, V> op) {
        try {
            return processIncrement(revision, op);
        } catch (MVCCStoreException e) {
            IncrementResultImpl<K, V> result = resultFactory.newIncrementResult(revision);
            result.code(e.getCode());
            return result;
        } catch (StateStoreRuntimeException e) {
            IncrementResultImpl<K, V> result = resultFactory.newIncrementResult(revision);
            result.code(Code.INTERNAL_ERROR);
            return result;
        }
    }

    synchronized IncrementResult<K, V> processIncrement(long revision, IncrementOp<K, V> op) {
        checkStoreOpen();

        WriteBatch batch = new WriteBatch();
        IncrementResult<K, V> result = null;
        try {
            result = increment(revision, batch, op);
            executeBatch(batch);
            return result;
        } catch (StateStoreRuntimeException e) {
            if (null != result) {
                result.close();
            }
            throw e;
        } finally {
            RocksUtils.close(batch);
        }
    }

    private IncrementResult<K, V> increment(long revision, WriteBatch batch, IncrementOp<K, V> op) {
        // parameters
        final K key = op.key();
        final long amount = op.amount();

        // raw key
        final byte[] rawKey = keyCoder.encode(key);

        MVCCRecord record;
        try {
            record = getKeyRecord(key, rawKey);
        } catch (StateStoreRuntimeException e) {
            throw e;
        }

        // result
        final IncrementResultImpl<K, V> result = resultFactory.newIncrementResult(revision);
        try {
            long oldAmount = 0L;
            if (null != record) {
                // validate the update revision before applying the update to the record
                if (record.compareModRev(revision) >= 0) {
                    result.code(Code.SMALLER_REVISION);
                    return result;
                }
                if (ValueType.NUMBER != record.getValueType()) {
                    result.code(Code.ILLEGAL_OP);
                    return result;
                }
                record.setVersion(record.getVersion() + 1);
                oldAmount = record.getValue().getLong(0);
            } else {
                record = MVCCRecord.newRecord();
                record.setCreateRev(revision);
                record.setVersion(0L);
                record.setValue(PooledByteBufAllocator.DEFAULT.buffer(Long.BYTES), ValueType.NUMBER);
            }
            long newAmount = oldAmount + amount;
            record.getValue().writerIndex(0);
            record.getValue().writeLong(newAmount);
            record.setModRev(revision);

            // write the mvcc record back
            batch.put(dataCfHandle, rawKey, recordCoder.encode(record));

            // finalize the result
            result.code(Code.OK);
            if (op.option().getTotal()) {
                result.totalAmount(newAmount);
            }
            return result;
        } catch (RocksDBException rde) {
            result.close();
            throw new StateStoreRuntimeException(rde);
        } catch (StateStoreRuntimeException e) {
            result.close();
            throw e;
        } finally {
            if (null != record) {
                record.recycle();
            }
        }
    }

    @Override
    public PutResult<K, V> put(long revision, PutOp<K, V> op) {
        try {
            return processPut(revision, op);
        } catch (MVCCStoreException e) {
            PutResultImpl<K, V> result = resultFactory.newPutResult(revision);
            result.code(e.getCode());
            return result;
        } catch (StateStoreRuntimeException e) {
            PutResultImpl<K, V> result = resultFactory.newPutResult(revision);
            result.code(Code.INTERNAL_ERROR);
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
                result.close();
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
                    result.code(Code.SMALLER_REVISION);
                    return result;
                }

                if (ValueType.BYTES != record.getValueType()) {
                    result.code(Code.ILLEGAL_OP);
                    return result;
                }

                if (op.option().prevKv()) {
                    // make a copy before modification
                    oldRecord = record.duplicate();
                }
                record.setVersion(record.getVersion() + 1);
            } else {
                record = MVCCRecord.newRecord();
                record.setCreateRev(revision);
                record.setVersion(0);
            }
            record.setValue(rawValBuf, ValueType.BYTES);
            record.setModRev(revision);

            // write the mvcc record back
            batch.put(dataCfHandle, rawKey, recordCoder.encode(record));

            // finalize the result
            result.code(Code.OK);
            if (null != oldRecord) {
                KeyValueImpl<K, V> prevKV = oldRecord.asKVRecord(
                        recordFactory,
                        key,
                        valCoder);
                result.prevKv(prevKV);
            }
            return result;
        } catch (StateStoreRuntimeException e) {
            result.close();
            throw e;
        } catch (RocksDBException e) {
            result.close();
            throw new StateStoreRuntimeException(e);
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
    public DeleteResult<K, V> delete(long revision, DeleteOp<K, V> op) {
        try {
            return processDelete(revision, op);
        } catch (MVCCStoreException e) {
            DeleteResultImpl<K, V> result = resultFactory.newDeleteResult(revision);
            result.code(e.getCode());
            return result;
        } catch (StateStoreRuntimeException e) {
            DeleteResultImpl<K, V> result = resultFactory.newDeleteResult(revision);
            result.code(Code.INTERNAL_ERROR);
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
                result.close();
            }
            throw e;
        } finally {
            RocksUtils.close(batch);
        }
    }

    DeleteResult<K, V> delete(long revision, WriteBatch batch, DeleteOp<K, V> op, boolean allowBlind) {
        // parameters
        final K key = op.key();
        final K endKey = op.option().endKey();
        final boolean blind = allowBlind && !op.option().prevKv();

        final byte[] rawKey = (null != key) ? keyCoder.encode(key) : NULL_START_KEY;
        final byte[] rawEndKey = (null != endKey) ? keyCoder.encode(endKey) : null;

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

            List<KeyValue<K, V>> kvs = toKvs(keys, records);

            result.code(Code.OK);
            result.prevKvs(kvs);
            result.numDeleted(numDeleted);
        } catch (StateStoreRuntimeException e) {
            result.close();
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
        try {
            if (null == endKey) {
                batch.delete(key);
            } else {
                Pair<byte[], byte[]> realRange = getRealRange(key, endKey);
                endKey = realRange.getRight();
                ++endKey[endKey.length - 1];
                batch.deleteRange(realRange.getLeft(), endKey);
            }
        } catch (RocksDBException e) {
            throw new StateStoreRuntimeException(e);
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
                try {
                    batch.delete(rawKey);
                } catch (RocksDBException e) {
                    throw new StateStoreRuntimeException(e);
                }
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
                null,
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
    public synchronized TxnResult<K, V> txn(long revision, TxnOp<K, V> op) {
        try {
            return processTxn(revision, op);
        } catch (MVCCStoreException e) {
            TxnResultImpl<K, V> result = resultFactory.newTxnResult(revision);
            result.code(e.getCode());
            return result;
        } catch (StateStoreRuntimeException e) {
            TxnResultImpl<K, V> result = resultFactory.newTxnResult(revision);
            result.code(Code.INTERNAL_ERROR);
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
        if (operations == null) {
            operations = Collections.emptyList();
        }
        results = Lists.newArrayListWithExpectedSize(operations.size());

        // 3. process the operations
        try (WriteBatch batch = new WriteBatch()) {
            for (Op<K, V> o : operations) {
                results.add(executeOp(revision, batch, o));
            }
            executeBatch(batch);

            // 4. repare the result
            TxnResultImpl<K, V> txnResult = resultFactory.newTxnResult(revision);
            txnResult.isSuccess(success);
            txnResult.results(results);
            txnResult.code(Code.OK);

            return txnResult;
        } catch (StateStoreRuntimeException e) {
            results.forEach(Result::close);
            throw e;
        }

    }

    boolean processCompareOp(CompareOp<K, V> op) {
        MVCCRecord record = null;
        K key = op.key();
        byte[] rawKey = keyCoder.encode(key);
        try {
            record = getKeyRecord(key, rawKey);
            if (null == record) {
                if (CompareTarget.VALUE != op.target()) {
                    throw new MVCCStoreException(Code.KEY_NOT_FOUND,
                        "Key '" + TextFormat.escapeBytes(rawKey) + "' is not found");
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
        switch (op.target()) {
            case MOD:
                cmp = record.compareModRev(op.revision());
                break;
            case CREATE:
                cmp = record.compareCreateRev(op.revision());
                break;
            case VERSION:
                cmp = record.compareVersion(op.revision());
                break;
            case VALUE:
                if (null == record) { // key not found
                    if (CompareResult.EQUAL == op.result()) {
                        return op.value() == null;
                    } else if (CompareResult.NOT_EQUAL == op.result()) {
                        return op.value() != null;
                    } else {
                        return false;
                    }
                }
                // key is found and value-to-compare is present
                if (op.value() != null) {
                    byte[] rawValue = valCoder.encode(op.value());
                    cmp = record.getValue().compareTo(Unpooled.wrappedBuffer(rawValue));
                } else {
                    // key is found but value-to-compare is missing
                    switch (op.result()) {
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
        switch (op.result()) {
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
        } else if (op instanceof RangeOp) {
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
                                  RangeOption<K> rangeOption,
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

                processRecord(key, val, resultKeys, resultValues, numKvs, rangeOption, countOnly);

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
                               RangeOption<K> rangeOption,
                               boolean countOnly) {
        if (null == rangeOption && countOnly) {
            numKvs.increment();
            return;
        }

        if (record.test(rangeOption)) {
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
            RangeResultImpl<K, V> result = resultFactory.newRangeResult(-1L);
            result.code(e.getCode());
            return result;
        } catch (StateStoreRuntimeException e) {
            RangeResultImpl<K, V> result = resultFactory.newRangeResult(-1L);
            result.code(Code.INTERNAL_ERROR);
            return result;
        }
    }

    synchronized RangeResult<K, V> processRange(RangeOp<K, V> rangeOp) {
        checkStoreOpen();

        // parameters
        final K key = rangeOp.key();
        final K endKey = rangeOp.option().endKey();

        // result
        final RangeResultImpl<K, V> result = resultFactory.newRangeResult(-1L);

        // raw key
        byte[] rawKey = (null != key) ? keyCoder.encode(key) : NULL_START_KEY;

        if (null == endKey) {
            // point lookup
            MVCCRecord record = getKeyRecord(key, rawKey);
            try {
                if (null == record || !record.test(rangeOp.option())) {
                    result.count(0);
                    result.kvs(Collections.emptyList());
                } else {
                    result.count(1);
                    result.kvs(Lists.newArrayList(record.asKVRecord(
                        recordFactory,
                        key,
                        valCoder)));
                }
                result.more(false);
                result.code(Code.OK);
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
                rangeOp.option(),
                rangeOp.option().limit(),
                false);

            List<KeyValue<K, V>> kvs = toKvs(keys, records);

            result.code(Code.OK);
            result.kvs(kvs);
            result.count(kvs.size());
            result.more(hasMore);
        } finally {
            records.forEach(MVCCRecord::recycle);
        }
        return result;
    }

    private List<KeyValue<K, V>> toKvs(List<byte[]> keys, List<MVCCRecord> records) {
        List<KeyValue<K, V>> kvs = Lists.newArrayListWithExpectedSize(keys.size());

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
