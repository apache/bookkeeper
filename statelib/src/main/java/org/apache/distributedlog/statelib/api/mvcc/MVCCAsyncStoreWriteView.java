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
import org.apache.distributedlog.statelib.api.mvcc.op.DeleteOp;
import org.apache.distributedlog.statelib.api.mvcc.op.PutOp;
import org.apache.distributedlog.statelib.api.mvcc.op.TxnOp;
import org.apache.distributedlog.statelib.api.mvcc.result.DeleteResult;
import org.apache.distributedlog.statelib.api.mvcc.result.PutResult;
import org.apache.distributedlog.statelib.api.mvcc.result.TxnResult;

/**
 * The write view of a mvcc key/value store that supports write operations, such as put and delete.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@Public
@Evolving
public interface MVCCAsyncStoreWriteView<K, V> {

    CompletableFuture<Void> put(K k, V v);

    CompletableFuture<V> putIfAbsent(K k, V v);

    CompletableFuture<Long> vPut(K k, V v, long expectedVersion);

    CompletableFuture<Long> rPut(K k, V v, long expectedRevision);

    CompletableFuture<V> delete(K k);

    CompletableFuture<Boolean> delete(K k, V v);

    CompletableFuture<List<KVRecord<K, V>>> deleteRange(K key, K endKey);

    CompletableFuture<KVRecord<K, V>> vDelete(K k, long expectedVersion);

    CompletableFuture<KVRecord<K, V>> rDelete(K k, long expectedRevision);

    CompletableFuture<PutResult<K, V>> put(PutOp<K, V> op);

    CompletableFuture<DeleteResult<K, V>> delete(DeleteOp<K, V> op);

    CompletableFuture<TxnResult<K, V>> txn(TxnOp<K, V> op);


}
