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
package org.apache.distributedlog.statestore.impl.kv;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.statestore.kv.KVAsyncStore;
import org.apache.distributedlog.api.statestore.kv.KVStore;
import org.apache.distributedlog.statestore.impl.journal.AbstractStateStoreWithJournal;
import org.apache.distributedlog.statestore.impl.journal.CommandProcessor;

/**
 * A async kv store implementation.
 */
public class RocksdbKVAsyncStoreImpl<K, V>
        extends AbstractStateStoreWithJournal<KVStore<K, V>>
        implements KVAsyncStore<K, V> {

    private static final byte[] CATCHUP_MARKER = new byte[0];

    public RocksdbKVAsyncStoreImpl(Supplier<KVStore<K, V>> localStateStoreSupplier,
                                   Supplier<Namespace> namespaceSupplier) {
        super(localStateStoreSupplier, namespaceSupplier);
    }

    @Override
    public CompletableFuture<V> get(K key) {
        return null;
    }

    @Override
    public CompletableFuture<Void> put(K key, V value) {
        return null;
    }

    @Override
    public CompletableFuture<V> putIfAbsent(K key, V value) {
        return null;
    }

    @Override
    public CompletableFuture<V> delete(K key) {
        return null;
    }

    //
    // Journaled State Store interfaces
    //

    @Override
    protected ByteBuf newCatchupMarker() {
        return Unpooled.wrappedBuffer(CATCHUP_MARKER);
    }

    @Override
    protected CommandProcessor<KVStore<K, V>> newCommandProcessor(KVStore<K, V> localStore) {
        return null;
    }
}
