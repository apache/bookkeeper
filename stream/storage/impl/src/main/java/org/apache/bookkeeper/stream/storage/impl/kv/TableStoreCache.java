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
package org.apache.bookkeeper.stream.storage.impl.kv;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.apache.bookkeeper.stream.storage.api.kv.TableStore;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;

/**
 * A cache for managing table stores.
 */
public class TableStoreCache {

    private final MVCCStoreFactory mvccStoreFactory;
    private final TableStoreFactory tableStoreFactory;
    private final ConcurrentMap<RangeId, TableStore> tableStores;
    private final ConcurrentMap<RangeId, CompletableFuture<TableStore>> tableStoresOpening;

    public TableStoreCache(MVCCStoreFactory mvccStoreFactory) {
        this(mvccStoreFactory, store -> new TableStoreImpl(store));
    }

    public TableStoreCache(MVCCStoreFactory mvccStoreFactory,
                           TableStoreFactory tableStoreFactory) {
        this.mvccStoreFactory = mvccStoreFactory;
        this.tableStoreFactory = tableStoreFactory;
        this.tableStores = Maps.newConcurrentMap();
        this.tableStoresOpening = Maps.newConcurrentMap();
    }

    @VisibleForTesting
    public ConcurrentMap<RangeId, TableStore> getTableStores() {
        return tableStores;
    }

    @VisibleForTesting
    ConcurrentMap<RangeId, CompletableFuture<TableStore>> getTableStoresOpening() {
        return tableStoresOpening;
    }

    public TableStore getTableStore(RangeId rid) {
        return tableStores.get(rid);
    }

    public CompletableFuture<TableStore> openTableStore(long scId, RangeId rid) {
        TableStore store = tableStores.get(rid);
        if (null != store) {
            return FutureUtils.value(store);
        }

        CompletableFuture<TableStore> openFuture = tableStoresOpening.get(rid);
        if (null != openFuture) {
            return openFuture;
        }

        // no store is cached, and there is no outstanding open request
        openFuture = FutureUtils.createFuture();
        CompletableFuture<TableStore> existingOpenFuture = tableStoresOpening.putIfAbsent(rid, openFuture);
        if (null != existingOpenFuture) {
            // there is already an ongoing open request
            return existingOpenFuture;
        }

        // I am the first one to open a table store
        final CompletableFuture<TableStore> openingFuture = openFuture;
        mvccStoreFactory.openStore(scId, rid.getStreamId(), rid.getRangeId())
            .thenAccept(mvccStore -> {
                TableStore newStore = tableStoreFactory.createStore(mvccStore);
                TableStore oldStore = tableStores.putIfAbsent(rid, newStore);
                if (null != oldStore) {
                    openingFuture.complete(oldStore);
                } else {
                    openingFuture.complete(newStore);
                }
                tableStoresOpening.remove(rid, openingFuture);
            })
            .exceptionally(cause -> {
                openingFuture.completeExceptionally(cause);
                tableStoresOpening.remove(rid, openingFuture);
                return null;
            });
        return openingFuture;
    }


}
