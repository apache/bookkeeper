/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.api.kv;

import static io.netty.util.ReferenceCountUtil.retain;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.kv.exceptions.KvApiException;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.IncrementOption;
import org.apache.bookkeeper.api.kv.options.Options;
import org.apache.bookkeeper.api.kv.options.PutOption;
import org.apache.bookkeeper.api.kv.result.Code;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.api.kv.result.IncrementResult;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.api.kv.result.PutResult;
import org.apache.bookkeeper.api.kv.result.RangeResult;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Write view of a given key space.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PTableWriteView<K, V> extends PTableBase<K, V> {

    CompletableFuture<PutResult<K, V>> put(K pKey, K lKey, V value, PutOption<K> option);

    CompletableFuture<DeleteResult<K, V>> delete(K pKey, K lKey, DeleteOption<K> option);

    CompletableFuture<IncrementResult<K, V>> increment(K pKey, K lKey, long amount, IncrementOption<K> option);

    Txn<K, V> txn(K pKey);

    default CompletableFuture<Void> increment(K pKey, K lKey, long amount) {
        return increment(pKey, lKey, amount, Options.blindIncrement())
            .thenApply(result -> {
                try {
                    return (Void) null;
                } finally {
                    result.close();
                }
            });
    }

    default CompletableFuture<Long> incrementAndGet(K pKey, K lKey, long amount) {
        return increment(pKey, lKey, amount, Options.incrementAndGet())
            .thenApply(result -> {
                try {
                    return result.totalAmount();
                } finally {
                    result.close();
                }
            });
    }

    default CompletableFuture<Long> getAndIncrement(K pKey, K lKey, long amount) {
        return increment(pKey, lKey, amount, Options.incrementAndGet())
            .thenApply(result -> {
                try {
                    return result.totalAmount() - amount;
                } finally {
                    result.close();
                }
            });
    }

    default CompletableFuture<Void> put(K pKey, K lKey, V value) {
        if (value == null) {
            return delete(pKey, lKey).thenApply(ignored -> null);
        }

        return put(pKey, lKey, value, Options.blindPut())
            .thenApply(result -> {
                try {
                    return (Void) null;
                } finally {
                    result.close();
                }
            });
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

    default CompletableFuture<V> delete(K pKey, K lKey) {
        return delete(pKey, lKey, Options.deleteAndGet())
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
            });
    }

    default CompletableFuture<List<KeyValue<K, V>>> deleteRange(K pKey, K lStartKey, K lEndKey) {
        DeleteOption<K> option = opFactory().optionFactory().newDeleteOption()
            .prevKv(true)
            .endKey(lEndKey)
            .build();
        return delete(pKey, lStartKey, option)
            .thenApply(result -> {
                try {
                    return result.getPrevKvsAndClear();
                } finally {
                    result.close();
                }
            })
            .whenComplete((v, cause) -> option.close());
    }

    default CompletableFuture<Boolean> delete(K pKey, K lKey, V value) {
        Txn<K, V> txn = txn(pKey);
        txn
            .If(
                opFactory().compareValue(CompareResult.EQUAL, lKey, value))
            .Then(
                opFactory().newDelete(
                    lKey,
                    Options.deleteAndGet()));
        return txn.commit()
            .thenApply(result -> {
                try {
                    return result.isSuccess() && !result.results().isEmpty();
                } finally {
                    result.close();
                }
            });
    }

    default CompletableFuture<KeyValue<K, V>> vDelete(K pKey, K lKey, long expectedVersion) {
        Txn<K, V> txn = txn(pKey);
        txn
            .If(
                opFactory().compareVersion(CompareResult.EQUAL, lKey, expectedVersion))
            .Then(
                opFactory().newDelete(
                    lKey,
                    Options.deleteAndGet()));
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
                                "Failed to vDelete key " + lKey + " (version = " + expectedVersion + ")"));
                    }
                } finally {
                    result.close();
                }
            });
    }

    default CompletableFuture<KeyValue<K, V>> rDelete(K pKey, K lKey, long expectedRevision) {
        Txn<K, V> txn = txn(pKey);
        txn
            .If(
                opFactory().compareModRevision(CompareResult.EQUAL, lKey, expectedRevision))
            .Then(
                opFactory().newDelete(
                    lKey,
                    Options.deleteAndGet()));
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
                                "Failed to rDelete key " + lKey + " (mod_rev = " + expectedRevision + ")"));
                    }
                } finally {
                    result.close();
                }
            });
    }

}
