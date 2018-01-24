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

package org.apache.distributedlog.api.kv;

import static io.netty.util.ReferenceCountUtil.retain;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.api.kv.exceptions.KvApiException;
import org.apache.distributedlog.api.kv.op.CompareResult;
import org.apache.distributedlog.api.kv.options.DeleteOption;
import org.apache.distributedlog.api.kv.options.PutOption;
import org.apache.distributedlog.api.kv.result.Code;
import org.apache.distributedlog.api.kv.result.DeleteResult;
import org.apache.distributedlog.api.kv.result.KeyValue;
import org.apache.distributedlog.api.kv.result.PutResult;
import org.apache.distributedlog.api.kv.result.RangeResult;

/**
 * Write view of a given key space.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TableWriteView<K, V> extends PTableBase<K, V> {

    CompletableFuture<PutResult<K, V>> put(K key, V value, PutOption<K> option);

    CompletableFuture<DeleteResult<K, V>> delete(K key, DeleteOption<K> option);

    CompletableFuture<Void> increment(K key, long amount);

    Txn<K, V> txn(K key);

    default CompletableFuture<Void> put(K key, V value) {
        if (value == null) {
            return delete(key).thenApply(ignored -> null);
        }

        PutOption<K> option = opFactory().optionFactory().newPutOption()
            .prevKv(false)
            .build();
        return put(key, value, option)
            .thenApply(result -> {
                try {
                    return (Void) null;
                } finally {
                    result.close();
                }
            })
            .whenComplete((v, cause) -> option.close());

    }

    default CompletableFuture<V> putIfAbsent(K pKey, K lKey, V value) {
        Txn<K, V> txn = txn(pKey);
        txn
            .If(
                opFactory().compareValue(CompareResult.EQUAL, lKey, null))
            .Then(
                opFactory().newPut(
                    lKey,
                    value,
                    opFactory().optionFactory().newPutOption().build()))
            .Else(
                newGet(lKey));
        return txn.commit()
            .thenCompose(result -> {
                try {
                    if (result.isSuccess()) {
                        return FutureUtils.value(null);
                    } else {
                        RangeResult<K, V> rangeResult = (RangeResult<K, V>) result.results().get(0);
                        if (rangeResult.kvs().isEmpty()) {
                            return FutureUtils.exception(
                                new KvApiException(
                                    Code.UNEXPECTED,
                                    "Key " + lKey + " not found when putIfAbsent failed"));
                        } else {
                            return FutureUtils.value(retain(rangeResult.kvs().get(0).value()));
                        }
                    }
                } finally {
                    result.close();
                }
            });
    }

    default CompletableFuture<Long> vPut(K pKey, K lKey, V value, long expectedVersion) {
        Txn<K, V> txn = txn(pKey);
        txn
            .If(
                opFactory().compareVersion(CompareResult.EQUAL, lKey, expectedVersion))
            .Then(
                opFactory().newPut(
                    lKey,
                    value,
                    opFactory().optionFactory().newPutOption().build()));
        return txn.commit()
            .thenCompose(result -> {
                try {
                    if (result.isSuccess()) {
                        return FutureUtils.value(expectedVersion + 1);
                    } else {
                        return FutureUtils.exception(
                            new KvApiException(
                                Code.BAD_REVISION,
                                "Failed to vPut(" + lKey + ", " + value + ")@version=" + expectedVersion));
                    }
                } finally {
                    result.close();
                }
            });
    }

    default CompletableFuture<V> delete(K key) {
        DeleteOption<K> option = opFactory().optionFactory().newDeleteOption()
            .prevKv(true)
            .endKey(null)
            .build();
        return delete(key, option)
            .thenApply(result -> {
                try {
                    List<KeyValue<K, V>> prevKvs = result.prevKvs();
                    if (prevKvs.isEmpty()) {
                        return null;
                    } else {
                        return retain(prevKvs.get(0).value());
                    }
                } finally {
                    result.close();
                }
            })
            .whenComplete((v, cause) -> option.close());
    }

    default CompletableFuture<Boolean> delete(K key, V value) {
        Txn<K, V> txn = txn(key);
        txn
            .If(
                opFactory().compareValue(CompareResult.EQUAL, key, value))
            .Then(
                opFactory().newDelete(
                    key,
                    opFactory().optionFactory().newDeleteOption()
                        .endKey(null)
                        .prevKv(true)
                        .build()));
        return txn.commit()
            .thenApply(result -> {
                try {
                    return result.isSuccess() && !result.results().isEmpty();
                } finally {
                    result.close();
                }
            });
    }

    default CompletableFuture<KeyValue<K, V>> vDelete(K key, long expectedVersion) {
        Txn<K, V> txn = txn(key);
        txn
            .If(
                opFactory().compareVersion(CompareResult.EQUAL, key, expectedVersion))
            .Then(
                opFactory().newDelete(
                    key,
                    opFactory().optionFactory().newDeleteOption()
                        .endKey(null)
                        .prevKv(true)
                        .build()));
        return txn.commit()
            .thenCompose(result -> {
                try {
                    if (result.isSuccess()) {
                        DeleteResult<K, V> deleteResult = (DeleteResult<K, V>) result.results().get(0);
                        if (deleteResult.prevKvs().isEmpty()) {
                            return FutureUtils.value(null);
                        } else {
                            List<KeyValue<K, V>> prevKvs = deleteResult.getPrevKvsAndClear();
                            return FutureUtils.value(prevKvs.get(0));
                        }
                    } else {
                        return FutureUtils.exception(
                            new KvApiException(Code.BAD_REVISION,
                                "Failed to vDelete key " + key + " (version = " + expectedVersion + ")"));
                    }
                } finally {
                    result.close();
                }
            });
    }

    default CompletableFuture<KeyValue<K, V>> rDelete(K key, long expectedRevision) {
        Txn<K, V> txn = txn(key);
        txn
            .If(
                opFactory().compareModRevision(CompareResult.EQUAL, key, expectedRevision))
            .Then(
                opFactory().newDelete(
                    key,
                    opFactory().optionFactory().newDeleteOption()
                        .endKey(null)
                        .prevKv(true)
                        .build()));
        return txn.commit()
            .thenCompose(result -> {
                try {
                    if (result.isSuccess()) {
                        DeleteResult<K, V> deleteResult = (DeleteResult<K, V>) result.results().get(0);
                        if (deleteResult.prevKvs().isEmpty()) {
                            return FutureUtils.value(null);
                        } else {
                            List<KeyValue<K, V>> prevKvs = deleteResult.getPrevKvsAndClear();
                            return FutureUtils.value(prevKvs.get(0));
                        }
                    } else {
                        return FutureUtils.exception(
                            new KvApiException(Code.BAD_REVISION,
                                "Failed to rDelete key " + key + " (mod_rev = " + expectedRevision + ")"));
                    }
                } finally {
                    result.close();
                }
            });
    }

}
