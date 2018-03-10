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
package org.apache.bookkeeper.statelib.impl.kv;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.bookkeeper.common.coder.Coder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.proto.statestore.kv.Command;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.api.exceptions.InvalidStateStoreException;
import org.apache.bookkeeper.statelib.api.kv.KVAsyncStore;
import org.apache.bookkeeper.statelib.impl.journal.AbstractStateStoreWithJournal;
import org.apache.bookkeeper.statelib.impl.journal.CommandProcessor;
import org.apache.distributedlog.api.namespace.Namespace;

/**
 * A async kv store implementation.
 */
public class RocksdbKVAsyncStore<K, V>
    extends AbstractStateStoreWithJournal<RocksdbKVStore<byte[], byte[]>>
    implements KVAsyncStore<K, V> {

    private static final byte[] CATCHUP_MARKER = new byte[0];

    private Coder<K> keyCoder;
    private Coder<V> valCoder;

    public RocksdbKVAsyncStore(Supplier<RocksdbKVStore<byte[], byte[]>> localStateStoreSupplier,
                               Supplier<Namespace> namespaceSupplier) {
        super(localStateStoreSupplier, namespaceSupplier);
    }

    @SuppressWarnings("unchecked")
    @Override
    public CompletableFuture<Void> init(StateStoreSpec spec) {
        this.keyCoder = (Coder<K>) spec.getKeyCoder();
        this.valCoder = (Coder<V>) spec.getValCoder();

        return super.init(spec);
    }

    @Override
    public CompletableFuture<V> get(K key) {
        synchronized (this) {
            if (!isInitialized) {
                return uninitializedException();
            }
        }

        return executeReadIO(() -> {
            byte[] keyBytes = keyCoder.encode(key);
            byte[] valBytes = localStore.get(keyBytes);
            if (null == valBytes) {
                return null;
            } else {
                return KVUtils.deserialize(valCoder, Unpooled.wrappedBuffer(valBytes));
            }
        });
    }

    @Override
    public CompletableFuture<Void> put(K key, V value) {
        synchronized (this) {
            if (!isInitialized) {
                return uninitializedException();
            }
        }

        byte[] keyBytes = keyCoder.encode(key);
        byte[] valBytes = valCoder.encode(value);

        Command command = Command.newBuilder()
            .setPutReq(KVUtils.newPutRequest(keyBytes, valBytes))
            .build();
        return writeCommandReturnTxId(command).thenApplyAsync((revision) -> {
            ByteBuf serializedBuf = KVUtils.serialize(valBytes, revision);
            try {
                byte[] serializedBytes = ByteBufUtil.getBytes(serializedBuf);
                localStore.put(keyBytes, serializedBytes, revision);
            } finally {
                serializedBuf.release();
            }
            return null;
        }, writeIOScheduler);
    }

    @Override
    public CompletableFuture<V> putIfAbsent(K key, V value) {

        byte[] keyBytes = keyCoder.encode(key);
        byte[] valBytes = valCoder.encode(value);

        Command command = Command.newBuilder()
            .setPutIfAbsentReq(KVUtils.newPutIfAbsentRequest(keyBytes, valBytes))
            .build();
        return writeCommandReturnTxId(command).thenApplyAsync((revision) -> {
            ByteBuf serializedBuf = KVUtils.serialize(valBytes, revision);
            try {
                byte[] serializedBytes = ByteBufUtil.getBytes(serializedBuf);
                byte[] prevValue = localStore.putIfAbsent(keyBytes, serializedBytes, revision);
                if (null == prevValue) {
                    return null;
                } else {
                    return KVUtils.deserialize(valCoder, Unpooled.wrappedBuffer(prevValue));
                }
            } finally {
                serializedBuf.release();
            }
        }, writeIOScheduler);
    }

    @Override
    public CompletableFuture<V> delete(K key) {
        byte[] keyBytes = keyCoder.encode(key);

        Command command = Command.newBuilder()
            .setDelReq(KVUtils.newDeleteRequest(keyBytes))
            .build();
        return writeCommandReturnTxId(command).thenApplyAsync((revision) -> {
            byte[] prevValue = localStore.delete(keyBytes, revision);
            if (null == prevValue) {
                return null;
            } else {
                return KVUtils.deserialize(valCoder, Unpooled.wrappedBuffer(prevValue));
            }
        }, writeIOScheduler);
    }

    private <AnyT> CompletableFuture<AnyT> uninitializedException() {
        return FutureUtils.exception(
            new InvalidStateStoreException("State store " + name() + " is not initialized yet."));
    }

    //
    // Journaled State Store interfaces
    //

    @Override
    protected ByteBuf newCatchupMarker() {
        return Unpooled.wrappedBuffer(CATCHUP_MARKER);
    }

    @Override
    protected CommandProcessor<RocksdbKVStore<byte[], byte[]>> newCommandProcessor() {
        return KVCommandProcessor.of();
    }

    private CompletableFuture<Long> writeCommandReturnTxId(Command command) {
        try {
            ByteBuf cmdBuf = KVUtils.newCommandBuf(command);
            return writeCommandBufReturnTxId(cmdBuf);
        } catch (IOException e) {
            return FutureUtils.exception(e);
        }
    }
}
