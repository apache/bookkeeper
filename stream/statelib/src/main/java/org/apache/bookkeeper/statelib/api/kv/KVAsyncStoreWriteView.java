/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.statelib.api.kv;

import java.util.concurrent.CompletableFuture;

/**
 * The write view of a kv state store that support asynchronous write operations.
 */
public interface KVAsyncStoreWriteView<K, V> {

    CompletableFuture<Void> put(K key, V value);

    CompletableFuture<V> putIfAbsent(K key, V value);

    CompletableFuture<V> delete(K key);

}
