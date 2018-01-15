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
package org.apache.distributedlog.stream.storage.impl.store;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.coder.ByteArrayCoder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.statelib.StateStores;
import org.apache.distributedlog.statelib.api.StateStoreSpec;
import org.apache.distributedlog.statelib.api.checkpoint.CheckpointStore;
import org.apache.distributedlog.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.distributedlog.statelib.impl.rocksdb.checkpoint.fs.FSCheckpointManager;
import org.apache.distributedlog.stream.protocol.RangeId;

/**
 * A default implementation of {@link RangeStoreFactory}.
 */
@Slf4j
public class RangeStoreFactoryImpl implements RangeStoreFactory {

    // store supplier
    private final Supplier<MVCCAsyncStore<byte[], byte[]>> storeSupplier;
    // scheduler
    private final OrderedScheduler writeIOScheduler;
    private final OrderedScheduler readIOScheduler;
    private final OrderedScheduler checkpointScheduler;
    // dirs
    private final File[] localStateDirs;
    // checkpoint manager
    private final CheckpointStore checkpointStore;
    // stores
    private final Map<Long, Map<RangeId, MVCCAsyncStore<byte[], byte[]>>> stores;
    private boolean closed = false;

    public RangeStoreFactoryImpl(Namespace namespace,
                                 File[] localStoreDirs,
                                 OrderedScheduler writeIOScheduler,
                                 OrderedScheduler readIOScheduler,
                                 OrderedScheduler checkpointScheduler) {
        this.storeSupplier = StateStores.mvccKvBytesStoreSupplier(() -> namespace);
        this.writeIOScheduler = writeIOScheduler;
        this.readIOScheduler = readIOScheduler;
        this.checkpointScheduler = checkpointScheduler;
        this.localStateDirs = localStoreDirs;
        // TODO: change this cto dlog based checkpoint manager
        this.checkpointStore = new FSCheckpointManager(new File(localStoreDirs[0], "checkpoints"));
        this.stores = Maps.newHashMap();
    }

    private ScheduledExecutorService chooseWriteIOExecutor(long streamId) {
        return writeIOScheduler.chooseThread(streamId);
    }

    private ScheduledExecutorService chooseReadIOExecutor(long streamId) {
        return readIOScheduler.chooseThread(streamId);
    }

    private ScheduledExecutorService chooseCheckpointIOExecutor(long streamId) {
        return checkpointScheduler.chooseThread(streamId);
    }

    private File chooseLocalStoreDir(long streamId) {
        int idx = (int) (streamId % localStateDirs.length);
        return localStateDirs[idx];
    }

    static String normalizedName(long id) {
        return String.format("%018d", id);
    }

    static String streamName(long scId,
                             long streamId,
                             long rangeId) {
        return String.format(
            "%s/%018d/%018d/%018d",
            "streams",
            scId,
            streamId,
            rangeId);
    }

    private synchronized void addStore(long scId, long streamId, long rangeId,
                                       MVCCAsyncStore<byte[], byte[]> store) {
        Map<RangeId, MVCCAsyncStore<byte[], byte[]>> scStores = stores.get(scId);
        if (null == scStores) {
            scStores = Maps.newHashMap();
        }
        RangeId rid = RangeId.of(streamId, rangeId);
        MVCCAsyncStore<byte[], byte[]> oldStore = scStores.get(rid);
        if (null != oldStore) {
            store.closeAsync();
        } else {
            log.info("Add store (scId = {}, streamId = {}, rangeId = {})",
                scId, streamId, rangeId);
            scStores.put(rid, store);
        }
    }

    private synchronized MVCCAsyncStore<byte[], byte[]> getStore(long scId, long streamId, long rangeId) {
        Map<RangeId, MVCCAsyncStore<byte[], byte[]>> scStores = stores.get(scId);
        if (null == scStores) {
            return null;
        } else {
            RangeId rid = RangeId.of(streamId, rangeId);
            return scStores.get(rid);
        }
    }

    @Override
    public CompletableFuture<MVCCAsyncStore<byte[], byte[]>> openStore(long scId, long streamId, long rangeId) {
        MVCCAsyncStore<byte[], byte[]> store = getStore(scId, streamId, rangeId);
        if (null == store) {
            return newStore(scId, streamId, rangeId);
        } else {
            return FutureUtils.value(store);
        }
    }

    CompletableFuture<MVCCAsyncStore<byte[], byte[]>> newStore(long scId, long streamId, long rangeId) {
        synchronized (this) {
            if (closed) {
                return FutureUtils.exception(new ObjectClosedException("RangeStoreFactory"));
            }
        }

        MVCCAsyncStore<byte[], byte[]> store = storeSupplier.get();

        File targetDir = chooseLocalStoreDir(streamId);
        // used for store ranges
        Path rangeStorePath = Paths.get(
            targetDir.getAbsolutePath(),
            "ranges",
            normalizedName(scId),
            normalizedName(streamId),
            normalizedName(rangeId));
        String storeName = String.format(
            "%s/%s/%s",
            normalizedName(scId),
            normalizedName(streamId),
            normalizedName(rangeId));

        // build a spec
        StateStoreSpec spec = StateStoreSpec.builder()
            .name(storeName)
            .keyCoder(ByteArrayCoder.of())
            .valCoder(ByteArrayCoder.of())
            .localStateStoreDir(rangeStorePath.toFile())
            .stream(streamName(scId, streamId, rangeId))
            .writeIOScheduler(chooseWriteIOExecutor(streamId))
            .readIOScheduler(chooseReadIOExecutor(streamId))
            .checkpointStore(checkpointStore)
            .checkpointDuration(Duration.ofMinutes(15))
            .checkpointIOScheduler(chooseCheckpointIOExecutor(streamId))
            .build();

        return store.init(spec).thenApply(ignored -> {
            addStore(scId, streamId, rangeId, store);
            return store;
        });
    }

    @Override
    public CompletableFuture<Void> closeStores(long scId) {
        Map<RangeId, MVCCAsyncStore<byte[], byte[]>> scStores;
        synchronized (this) {
            scStores = stores.remove(scId);
        }
        if (null == scStores) {
            return FutureUtils.Void();
        }

        List<CompletableFuture<Void>> closeFutures = Lists.newArrayList();
        for (MVCCAsyncStore<byte[], byte[]> store : scStores.values()) {
            closeFutures.add(store.closeAsync());
        }

        return FutureUtils.collect(closeFutures).thenApply(ignored -> null);
    }

    @Override
    public void close() {
        Map<Long, Map<RangeId, MVCCAsyncStore<byte[], byte[]>>> storesToClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            storesToClose = Maps.newHashMap(stores);
            closed = true;
        }

        List<CompletableFuture<Void>> closeFutures = Lists.newArrayList();
        for (Map<RangeId, MVCCAsyncStore<byte[], byte[]>> scStores : storesToClose.values()) {
            for (MVCCAsyncStore<byte[], byte[]> store : scStores.values()) {
                closeFutures.add(store.closeAsync());
            }
        }
        try {
            FutureUtils.result(FutureUtils.collect(closeFutures));
            log.info("Successfully closed all the range stores opened by this range factory");
        } catch (Exception e) {
            log.info("Encountered issue on closing all the range stores opened by this range factory");
        }
        checkpointStore.close();
    }
}
