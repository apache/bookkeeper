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

package org.apache.distributedlog.statelib.api.mvcc;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.statelib.api.AsyncStateStore;
import org.apache.distributedlog.statelib.api.exceptions.MVCCStoreException;
import org.apache.distributedlog.statelib.api.mvcc.op.CompareOp;
import org.apache.distributedlog.statelib.api.mvcc.op.CompareResult;
import org.apache.distributedlog.statelib.api.mvcc.op.DeleteOp;
import org.apache.distributedlog.statelib.api.mvcc.op.IncrementOp;
import org.apache.distributedlog.statelib.api.mvcc.op.OpFactory;
import org.apache.distributedlog.statelib.api.mvcc.op.PutOp;
import org.apache.distributedlog.statelib.api.mvcc.op.RangeOp;
import org.apache.distributedlog.statelib.api.mvcc.op.TxnOp;
import org.apache.distributedlog.statelib.api.mvcc.op.TxnOpBuilder;
import org.apache.distributedlog.statelib.api.mvcc.result.Code;
import org.apache.distributedlog.statelib.api.mvcc.result.DeleteResult;
import org.apache.distributedlog.statelib.api.mvcc.result.RangeResult;
import org.apache.distributedlog.statelib.api.mvcc.result.Result;

/**
 * A mvcc store that supports asynchronous operations.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Public
@Evolving
public interface MVCCAsyncStore<K, V>
    extends AsyncStateStore,
            MVCCAsyncStoreWriteView<K, V>,
            MVCCAsyncStoreReadView<K, V> {

    static <T> CompletableFuture<T> failWithCode(Code code, String msg) {
        return FutureUtils.exception(new MVCCStoreException(code, msg));
    }

    /**
     * Return the operator factory to build operators.
     *
     * @return operator factory.
     */
    OpFactory<K, V> getOpFactory();

    default CompareOp<K, V> newCompareCreateRevision(CompareResult result, K key, long revision) {
        return getOpFactory().compareCreateRevision(result, key, revision);
    }

    default CompareOp<K, V> newCompareModRevision(CompareResult result, K key, long revision) {
        return getOpFactory().compareModRevision(result, key, revision);
    }

    default CompareOp<K, V> newCompareVersion(CompareResult result, K key, long version) {
        return getOpFactory().compareVersion(result, key, version);
    }

    default CompareOp<K, V> newCompareValue(CompareResult result, K key, V value) {
        return getOpFactory().compareValue(result, key, value);
    }

    default TxnOpBuilder<K, V> newTxn() {
        return getOpFactory().buildTxnOp();
    }

    default PutOp<K, V> newPut(K key, V value) {
        return getOpFactory().buildPutOp()
            .key(key)
            .value(value)
            .prevKV(false)
            .build();
    }

    default DeleteOp<K, V> newDelete(K key) {
        return getOpFactory().buildDeleteOp()
            .key(key)
            .isRangeOp(false)
            .build();
    }

    default DeleteOp<K, V> newDeleteRange(K key, K endKey) {
        return getOpFactory().buildDeleteOp()
            .key(key)
            .endKey(endKey)
            .isRangeOp(true)
            .build();
    }

    @Override
    default CompletableFuture<V> get(K key) {
        RangeOp<K, V> op = getOpFactory().buildRangeOp()
            .key(key)
            .isRangeOp(false)
            .build();
        return range(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    if (result.kvs().isEmpty()) {
                        return FutureUtils.value(null);
                    } else {
                        return FutureUtils.value(result.kvs().get(0).value());
                    }
                } else {
                    return failWithCode(result.code(), "Failed to retrieve key " + key + " from store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<KVRecord<K, V>> getDetail(K key) {
        RangeOp<K, V> op = getOpFactory().buildRangeOp()
            .key(key)
            .isRangeOp(false)
            .build();
        return range(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    if (result.kvs().isEmpty()) {
                        return FutureUtils.value(null);
                    }
                    List<KVRecord<K, V>> records = result.getKvsAndClear();
                    KVRecord<K, V> record = records.get(0);
                    return FutureUtils.value(record);
                } else {
                    return failWithCode(result.code(), "Failed to retrieve key " + key + " from store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<List<KVRecord<K, V>>> range(K key, K endKey) {
        RangeOp<K, V> op = getOpFactory().buildRangeOp()
            .nullableKey(key)
            .nullableEndKey(endKey)
            .isRangeOp(true)
            .build();
        return range(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    return FutureUtils.value(result.getKvsAndClear());
                } else {
                    return failWithCode(result.code(),
                        "Failed to retrieve range [" + key + ", " + endKey + "] from store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<Void> put(K k, V v) {
        if (v == null) {
            return delete(k).thenApply(ignored -> null);
        }

        PutOp<K, V> op = getOpFactory().buildPutOp()
            .key(k)
            .value(v)
            .prevKV(false)
            .build();
        return put(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    return FutureUtils.Void();
                } else {
                    return failWithCode(result.code(),
                        "Failed to put (" + k + ", " + v + ") to store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<V> putIfAbsent(K k, V v) {
        TxnOp<K, V> op = getOpFactory().buildTxnOp()
            .addCompareOps(
                getOpFactory().compareValue(CompareResult.EQUAL, k, null))
            .addSuccessOps(
                getOpFactory().buildPutOp()
                    .key(k)
                    .value(v)
                    .build())
            .addFailureOps(
                getOpFactory().buildRangeOp()
                    .key(k)
                    .nullableEndKey(null)
                    .isRangeOp(false)
                    .build())
            .build();
        return txn(op).thenCompose(result -> {
            try {
                Code code = result.code();
                if (Code.OK == code) {
                    if (result.isSuccess()) {
                        return FutureUtils.value(null);
                    } else {
                        RangeResult<K, V> rangeResult = (RangeResult<K, V>) result.results().get(0);
                        if (rangeResult.kvs().isEmpty()) {
                            return failWithCode(Code.UNEXPECTED,
                                "Key " + k + " not found when putIfAbsent failed at store " + name());
                        } else {
                            return FutureUtils.value(rangeResult.kvs().get(0).value());
                        }
                    }
                } else {
                    return failWithCode(result.code(),
                        "Failed to putIfAbsent (" + k + ", " + v + ") to store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<Long> vPut(K k, V v, long expectedVersion) {
        TxnOp<K, V> op = getOpFactory().buildTxnOp()
            .addCompareOps(
                getOpFactory().compareVersion(CompareResult.EQUAL, k, expectedVersion))
            .addSuccessOps(
                getOpFactory().buildPutOp()
                    .key(k)
                    .value(v)
                    .build())
            .build();

        return txn(op).thenCompose(result -> {
            try {
                Code code = result.code();
                if (Code.OK == code && !result.isSuccess()) {
                    code = Code.BAD_REVISION;
                }
                if (Code.OK == code) {
                    return FutureUtils.value(expectedVersion + 1);
                } else {
                    return failWithCode(result.code(),
                        "Failed to vPut (" + k + ", " + v + ")@version=" + expectedVersion + " to store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<Long> rPut(K k, V v, long expectedRevision) {
        TxnOp<K, V> op = getOpFactory().buildTxnOp()
            .addCompareOps(
                getOpFactory().compareModRevision(CompareResult.EQUAL, k, expectedRevision))
            .addSuccessOps(
                getOpFactory().buildPutOp()
                    .key(k)
                    .value(v)
                    .build())
            .build();

        return txn(op).thenCompose(result -> {
            try {
                Code code = result.code();
                if (Code.OK == code && !result.isSuccess()) {
                    code = Code.BAD_REVISION;
                }
                if (Code.OK == code) {
                    return FutureUtils.value(result.revision());
                } else {
                    return failWithCode(result.code(),
                        "Failed to vPut (" + k + ", " + v + ")@revision=" + expectedRevision + " to store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<V> delete(K k) {
        DeleteOp<K, V> op = getOpFactory().buildDeleteOp()
            .key(k)
            .prevKV(true)
            .build();
        return delete(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    List<KVRecord<K, V>> prevKvs = result.prevKvs();
                    if (prevKvs.isEmpty()) {
                        return FutureUtils.value(null);
                    } else {
                        return FutureUtils.value(prevKvs.get(0).value());
                    }
                } else {
                    return failWithCode(result.code(),
                        "Fail to delete key " + k + " from store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<List<KVRecord<K, V>>> deleteRange(K key, K endKey) {
        DeleteOp<K, V> op = getOpFactory().buildDeleteOp()
            .nullableKey(key)
            .nullableEndKey(endKey)
            .prevKV(true)
            .build();
        return delete(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    List<KVRecord<K, V>> prevKvs = result.getPrevKvsAndClear();
                    return FutureUtils.value(prevKvs);
                } else {
                    return failWithCode(result.code(),
                        "Fail to delete key range [" + key + ", " + endKey + "] from store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<KVRecord<K, V>> vDelete(K k, long expectedVersion) {
        TxnOp<K, V> op = getOpFactory().buildTxnOp()
            .addCompareOps(
                getOpFactory().compareVersion(CompareResult.EQUAL, k, expectedVersion))
            .addSuccessOps(
                getOpFactory().buildDeleteOp()
                    .key(k)
                    .prevKV(true)
                    .build())
            .build();

        return txn(op).thenCompose(result -> {
            try {
                Code code = result.code();
                if (Code.OK == code && !result.isSuccess()) {
                    code = Code.BAD_REVISION;
                }
                if (Code.OK == code) {
                    List<Result<K, V>> subResults = result.results();
                    DeleteResult<K, V> deleteResult = (DeleteResult<K, V>) subResults.get(0);
                    List<KVRecord<K, V>> prevKvs = deleteResult.getPrevKvsAndClear();
                    if (prevKvs.isEmpty()) {
                        return FutureUtils.value(null);
                    } else {
                        return FutureUtils.value(prevKvs.get(0));
                    }
                } else {
                    return failWithCode(code,
                        "Failed to vDelete key " + k + " (version=" + expectedVersion + ") to store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<KVRecord<K, V>> rDelete(K k, long expectedRevision) {
        TxnOp<K, V> op = getOpFactory().buildTxnOp()
            .addCompareOps(
                getOpFactory().compareModRevision(CompareResult.EQUAL, k, expectedRevision))
            .addSuccessOps(
                getOpFactory().buildDeleteOp()
                    .key(k)
                    .prevKV(true)
                    .build())
            .build();

        return txn(op).thenCompose(result -> {
            try {
                Code code = result.code();
                if (Code.OK == code && !result.isSuccess()) {
                    code = Code.BAD_REVISION;
                }
                if (Code.OK == code) {
                    List<Result<K, V>> subResults = result.results();
                    DeleteResult<K, V> deleteResult = (DeleteResult<K, V>) subResults.get(0);
                    List<KVRecord<K, V>> prevKvs = deleteResult.getPrevKvsAndClear();
                    if (prevKvs.isEmpty()) {
                        return FutureUtils.value(null);
                    } else {
                        return FutureUtils.value(prevKvs.get(0));
                    }
                } else {
                    return failWithCode(code,
                        "Failed to rDelete key " + k + " (mod_rev=" + expectedRevision + ") to store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<Boolean> delete(K k, V v) {
        TxnOp<K, V> op = getOpFactory().buildTxnOp()
            .addCompareOps(
                getOpFactory().compareValue(CompareResult.EQUAL, k, v))
            .addSuccessOps(
                getOpFactory().buildDeleteOp()
                    .key(k)
                    .prevKV(true)
                    .build())
            .build();

        return txn(op).thenCompose(result -> {
            try {
                Code code = result.code();
                if (Code.OK == code && !result.isSuccess()) {
                    code = Code.BAD_REVISION;
                }
                if (Code.OK == code) {
                    return FutureUtils.value(!result.results().isEmpty());
                } else if (Code.BAD_REVISION == code) {
                    return FutureUtils.value(false);
                } else {
                    return failWithCode(code,
                        "Failed to delete (" + k + ", " + v + ") to store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }

    @Override
    default CompletableFuture<Void> increment(K k, long amount) {
        IncrementOp<K, V> op = getOpFactory().buildIncrementOp()
            .key(k)
            .amount(amount)
            .build();
        return increment(op).thenCompose(result -> {
            try {
                Code code = result.code();
                if (Code.OK == code) {
                    return FutureUtils.Void();
                } else {
                    return failWithCode(code,
                        "Failed to increment(" + k + ", " + amount + ") to store " + name());
                }
            } finally {
                result.recycle();
            }
        });
    }
}
