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

package org.apache.bookkeeper.statelib;

import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.statelib.api.kv.KVAsyncStore;
import org.apache.bookkeeper.statelib.api.kv.KVStore;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.bookkeeper.statelib.impl.kv.RocksdbKVAsyncStore;
import org.apache.bookkeeper.statelib.impl.kv.RocksdbKVStore;
import org.apache.bookkeeper.statelib.impl.mvcc.MVCCStores;
import org.apache.distributedlog.api.namespace.Namespace;

/**
 * A central place for creating and managing state stores.
 */
@Public
@Evolving
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StateStores {

    /**
     * Returns a supplier that supplies simple kv store.
     *
     * @return a supplier that supplies simple kv store.
     */
    public static <K, V> Supplier<KVStore<K, V>> kvStoreSupplier() {
        return () ->
            new RocksdbKVStore<>();
    }

    /**
     * Returns a supplier that supplies simple kv async store.
     *
     * @param logNamespaceSupplier log namespace supplier
     * @return a supplier that supplies simple kv store.
     */
    public static <K, V> Supplier<KVAsyncStore<K, V>> kvAsyncStoreSupplier(Supplier<Namespace> logNamespaceSupplier) {
        return () ->
            new RocksdbKVAsyncStore<>(() -> new RocksdbKVStore<>(), logNamespaceSupplier);
    }

    /**
     * Returns a supplier that supplies mvcc bytes store.
     *
     * @param nsSupplier namespace supplier
     * @return a supplier that supplies mvcc bytes store.
     */
    public static Supplier<MVCCAsyncStore<byte[], byte[]>> mvccKvBytesStoreSupplier(Supplier<Namespace> nsSupplier) {
        return MVCCStores.bytesStoreSupplier(nsSupplier);
    }

}
