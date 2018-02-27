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

package org.apache.bookkeeper.statelib.api.mvcc;

import static io.netty.util.ReferenceCountUtil.retain;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.op.DeleteOp;
import org.apache.bookkeeper.api.kv.op.IncrementOp;
import org.apache.bookkeeper.api.kv.op.OpFactory;
import org.apache.bookkeeper.api.kv.op.PutOp;
import org.apache.bookkeeper.api.kv.op.RangeOp;
import org.apache.bookkeeper.api.kv.op.TxnOp;
import org.apache.bookkeeper.api.kv.op.TxnOpBuilder;
import org.apache.bookkeeper.api.kv.options.Options;
import org.apache.bookkeeper.api.kv.result.Code;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.api.kv.result.RangeResult;
import org.apache.bookkeeper.api.kv.result.Result;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.statelib.api.AsyncStateStore;
import org.apache.bookkeeper.statelib.api.exceptions.MVCCStoreException;

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
        return getOpFactory().newTxn();
    }

    default PutOp<K, V> newPut(K key, V value) {
        return getOpFactory().newPut(
            key,
            value,
            Options.blindPut());
    }

    default RangeOp<K, V> newGet(K key) {
        return getOpFactory().newRange(
            key,
            Options.get());
    }

    default RangeOp<K, V> newRange(K key, K endKey) {
        return getOpFactory().newRange(
            key,
            getOpFactory().optionFactory().newRangeOption()
                .endKey(endKey)
                .limit(-1)
                .build());
    }

    default DeleteOp<K, V> newDelete(K key) {
        return getOpFactory().newDelete(
            key,
            Options.delete());
    }

    default DeleteOp<K, V> newDeleteRange(K key, K endKey) {
        return getOpFactory().newDelete(
            key,
            getOpFactory().optionFactory().newDeleteOption()
                .endKey(endKey)
                .prevKv(false)
                .build());
    }

    default IncrementOp<K, V> newIncrement(K key, long amount) {
        return getOpFactory().newIncrement(
            key,
            amount,
            Options.blindIncrement());
    }

    default IncrementOp<K, V> newIncrementAndGet(K key, long amount) {
        return getOpFactory().newIncrement(
            key,
            amount,
            Options.incrementAndGet());
    }

    @Override
    default CompletableFuture<V> get(K key) {
        RangeOp<K, V> op = getOpFactory().newRange(
            key,
            Options.get());
        return range(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    if (result.kvs().isEmpty()) {
                        return FutureUtils.value(null);
                    } else {
                        return FutureUtils.value(retain(result.kvs().get(0).value()));
                    }
                } else {
                    return failWithCode(result.code(), "Failed to retrieve key " + key + " from store " + name());
                }
            } finally {
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<KeyValue<K, V>> getKeyValue(K key) {
        RangeOp<K, V> op = newGet(key);
        return range(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    if (result.kvs().isEmpty()) {
                        return FutureUtils.value(null);
                    }
                    List<KeyValue<K, V>> records = result.getKvsAndClear();
                    KeyValue<K, V> record = records.get(0);
                    return FutureUtils.value(record);
                } else {
                    return failWithCode(result.code(), "Failed to retrieve key " + key + " from store " + name());
                }
            } finally {
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<List<KeyValue<K, V>>> range(K key, K endKey) {
        RangeOp<K, V> op = newRange(key, endKey);
        return range(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    return FutureUtils.value(result.getKvsAndClear());
                } else {
                    return failWithCode(result.code(),
                        "Failed to retrieve range [" + key + ", " + endKey + "] from store " + name());
                }
            } finally {
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<Void> put(K k, V v) {
        if (v == null) {
            return delete(k).thenApply(ignored -> null);
        }

        PutOp<K, V> op = newPut(k, v);
        return put(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    return FutureUtils.Void();
                } else {
                    return failWithCode(result.code(),
                        "Failed to put (" + k + ", " + v + ") to store " + name());
                }
            } finally {
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<V> putIfAbsent(K k, V v) {
        TxnOp<K, V> op = getOpFactory().newTxn()
            .If(newCompareValue(CompareResult.EQUAL, k, null))
            .Then(newPut(k, v))
            .Else(newGet(k))
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
                            return FutureUtils.value(retain(rangeResult.kvs().get(0).value()));
                        }
                    }
                } else {
                    return failWithCode(result.code(),
                        "Failed to putIfAbsent (" + k + ", " + v + ") to store " + name());
                }
            } finally {
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<Long> vPut(K k, V v, long expectedVersion) {
        TxnOp<K, V> op = getOpFactory().newTxn()
            .If(newCompareVersion(CompareResult.EQUAL, k, expectedVersion))
            .Then(newPut(k, v))
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
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<Long> rPut(K k, V v, long expectedRevision) {
        TxnOp<K, V> op = getOpFactory().newTxn()
            .If(newCompareModRevision(CompareResult.EQUAL, k, expectedRevision))
            .Then(newPut(k, v))
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
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<V> delete(K k) {
        DeleteOp<K, V> op = getOpFactory().newDelete(
            k,
            Options.deleteAndGet());
        return delete(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    List<KeyValue<K, V>> prevKvs = result.prevKvs();
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
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<List<KeyValue<K, V>>> deleteRange(K key, K endKey) {
        DeleteOp<K, V> op = getOpFactory().newDelete(
            key,
            getOpFactory().optionFactory().newDeleteOption()
                .endKey(endKey)
                .prevKv(true)
                .build());
        return delete(op).thenCompose(result -> {
            try {
                if (Code.OK == result.code()) {
                    List<KeyValue<K, V>> prevKvs = result.getPrevKvsAndClear();
                    return FutureUtils.value(prevKvs);
                } else {
                    return failWithCode(result.code(),
                        "Fail to delete key range [" + key + ", " + endKey + "] from store " + name());
                }
            } finally {
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<KeyValue<K, V>> vDelete(K k, long expectedVersion) {
        TxnOp<K, V> op = getOpFactory().newTxn()
            .If(newCompareVersion(CompareResult.EQUAL, k, expectedVersion))
            .Then(getOpFactory().newDelete(
                k,
                Options.deleteAndGet()))
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
                    List<KeyValue<K, V>> prevKvs = deleteResult.getPrevKvsAndClear();
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
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<KeyValue<K, V>> rDelete(K k, long expectedRevision) {
        TxnOp<K, V> op = getOpFactory().newTxn()
            .If(newCompareModRevision(CompareResult.EQUAL, k, expectedRevision))
            .Then(getOpFactory().newDelete(
                k,
                Options.deleteAndGet()))
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
                    List<KeyValue<K, V>> prevKvs = deleteResult.getPrevKvsAndClear();
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
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<Boolean> delete(K k, V v) {
        TxnOp<K, V> op = getOpFactory().newTxn()
            .If(newCompareValue(CompareResult.EQUAL, k, v))
            .Then(getOpFactory().newDelete(
                k,
                Options.deleteAndGet()))
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
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<Void> increment(K k, long amount) {
        IncrementOp<K, V> op = getOpFactory().newIncrement(
            k,
            amount,
            Options.blindIncrement());
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
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<Long> incrementAndGet(K k, long amount) {
        IncrementOp<K, V> op = getOpFactory().newIncrement(
            k,
            amount,
            Options.incrementAndGet());
        return increment(op).thenCompose(result -> {
            try {
                Code code = result.code();
                if (Code.OK == code) {
                    return FutureUtils.value(result.totalAmount());
                } else {
                    return failWithCode(code,
                        "Failed to increment(" + k + ", " + amount + ") to store " + name());
                }
            } finally {
                result.close();
            }
        });
    }

    @Override
    default CompletableFuture<Long> getAndIncrement(K k, long amount) {
        IncrementOp<K, V> op = getOpFactory().newIncrement(
            k,
            amount,
            Options.incrementAndGet());
        return increment(op).thenCompose(result -> {
            try {
                Code code = result.code();
                if (Code.OK == code) {
                    return FutureUtils.value(result.totalAmount() - amount);
                } else {
                    return failWithCode(code,
                        "Failed to increment(" + k + ", " + amount + ") to store " + name());
                }
            } finally {
                result.close();
            }
        });
    }
}
