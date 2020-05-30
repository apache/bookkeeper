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

package org.apache.bookkeeper.stream.storage.impl.service;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.CONTAINER_META_RANGE_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.CONTAINER_META_STREAM_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.ROOT_RANGE_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.ROOT_STREAM_ID;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.ResponseHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesRequest;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesResponse;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.protocol.RangeId;
import org.apache.bookkeeper.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.bookkeeper.stream.storage.api.kv.TableStore;
import org.apache.bookkeeper.stream.storage.api.metadata.MetaRangeStore;
import org.apache.bookkeeper.stream.storage.api.metadata.RangeStoreService;
import org.apache.bookkeeper.stream.storage.api.metadata.RootRangeStore;
import org.apache.bookkeeper.stream.storage.impl.kv.TableStoreCache;
import org.apache.bookkeeper.stream.storage.impl.kv.TableStoreFactory;
import org.apache.bookkeeper.stream.storage.impl.kv.TableStoreImpl;
import org.apache.bookkeeper.stream.storage.impl.metadata.MetaRangeStoreFactory;
import org.apache.bookkeeper.stream.storage.impl.metadata.MetaRangeStoreImpl;
import org.apache.bookkeeper.stream.storage.impl.metadata.RootRangeStoreFactory;
import org.apache.bookkeeper.stream.storage.impl.metadata.RootRangeStoreImpl;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;

/**
 * The service implementation running in a storage container.
 */
@Slf4j
class RangeStoreServiceImpl implements RangeStoreService, AutoCloseable {

    private final long scId;

    // store factory
    private final MVCCStoreFactory storeFactory;
    // storage container
    @Getter(value = AccessLevel.PACKAGE)
    private MetaRangeStore mgStore;
    @Getter(value = AccessLevel.PACKAGE)
    private final MetaRangeStoreFactory mrStoreFactory;
    // root range
    @Getter(value = AccessLevel.PACKAGE)
    private RootRangeStore rootRange;
    @Getter(value = AccessLevel.PACKAGE)
    private final RootRangeStoreFactory rrStoreFactory;
    // table range stores
    @Getter(value = AccessLevel.PACKAGE)
    private final TableStoreCache tableStoreCache;
    @Getter(value = AccessLevel.PACKAGE)
    private final TableStoreFactory tableStoreFactory;

    RangeStoreServiceImpl(long scId,
                          StorageContainerPlacementPolicy rangePlacementPolicy,
                          OrderedScheduler scheduler,
                          MVCCStoreFactory storeFactory,
                          StorageServerClientManager clientManager) {
        this(
            scId,
            scheduler,
            storeFactory,
            store -> new RootRangeStoreImpl(
                store, rangePlacementPolicy, scheduler.chooseThread(scId)),
            store -> new MetaRangeStoreImpl(store, rangePlacementPolicy, scheduler.chooseThread(scId), clientManager),
            store -> new TableStoreImpl(store));
    }

    RangeStoreServiceImpl(long scId,
                          OrderedScheduler scheduler,
                          MVCCStoreFactory storeFactory,
                          RootRangeStoreFactory rrStoreFactory,
                          MetaRangeStoreFactory mrStoreFactory,
                          TableStoreFactory tableStoreFactory) {
        this.scId = scId;
        RangeStoreService failRequestStorageContainer =
            FailRequestRangeStoreService.of(scheduler);
        this.rootRange = failRequestStorageContainer;
        this.mgStore = failRequestStorageContainer;
        this.storeFactory = storeFactory;
        this.rrStoreFactory = rrStoreFactory;
        this.mrStoreFactory = mrStoreFactory;
        this.tableStoreFactory = tableStoreFactory;
        this.tableStoreCache = new TableStoreCache(storeFactory, tableStoreFactory);
    }

    //
    // Services
    //

    public long getId() {
        return scId;
    }

    private CompletableFuture<Void> startRootRangeStore() {
        if (ROOT_STORAGE_CONTAINER_ID != scId) {
            return FutureUtils.Void();
        }
        return storeFactory.openStore(
            ROOT_STORAGE_CONTAINER_ID,
            ROOT_STREAM_ID,
            ROOT_RANGE_ID
        ).thenApply(store -> {
            rootRange = rrStoreFactory.createStore(store);
            return null;
        });
    }

    private CompletableFuture<Void> startMetaRangeStore(long scId) {
        return storeFactory.openStore(
            scId,
            CONTAINER_META_STREAM_ID,
            CONTAINER_META_RANGE_ID
        ).thenApply(store -> {
            mgStore = mrStoreFactory.createStore(store);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> start() {
        List<CompletableFuture<Void>> futures = Lists.newArrayList(
            startRootRangeStore(),
            startMetaRangeStore(scId));

        return FutureUtils.collect(futures).thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> stop() {
        return storeFactory.closeStores(scId);
    }

    @Override
    public void close() {
        stop().join();
    }

    //
    // Storage Container API
    //

    //
    // Namespace API
    //

    @Override
    public CompletableFuture<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest request) {
        return rootRange.createNamespace(request);
    }

    @Override
    public CompletableFuture<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest request) {
        return rootRange.deleteNamespace(request);
    }

    @Override
    public CompletableFuture<GetNamespaceResponse> getNamespace(GetNamespaceRequest request) {
        return rootRange.getNamespace(request);
    }

    //
    // Stream API.
    //

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(CreateStreamRequest request) {
        return rootRange.createStream(request);
    }

    @Override
    public CompletableFuture<DeleteStreamResponse> deleteStream(DeleteStreamRequest request) {
        return rootRange.deleteStream(request);
    }

    @Override
    public CompletableFuture<GetStreamResponse> getStream(GetStreamRequest request) {
        return rootRange.getStream(request);
    }

    //
    // Stream Meta Range API.
    //

    @Override
    public CompletableFuture<GetActiveRangesResponse> getActiveRanges(GetActiveRangesRequest request) {
        return mgStore.getActiveRanges(request);
    }

    //
    // Table API
    //


    @Override
    public CompletableFuture<RangeResponse> range(RangeRequest request) {
        RoutingHeader header = request.getHeader();

        if (header.getRangeId() <= 0L) {
            return CompletableFuture.completedFuture(RangeResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setCode(StatusCode.BAD_REQUEST)
                    .setRoutingHeader(request.getHeader())
                    .build())
                .build());
        }

        RangeId rid = RangeId.of(header.getStreamId(), header.getRangeId());
        TableStore store = tableStoreCache.getTableStore(rid);
        if (null != store) {
            return store.range(request);
        } else {
            return tableStoreCache.openTableStore(scId, rid)
                .thenCompose(s -> s.range(request));
        }
    }

    @Override
    public CompletableFuture<PutResponse> put(PutRequest request) {
        RoutingHeader header = request.getHeader();

        if (header.getRangeId() <= 0L) {
            return CompletableFuture.completedFuture(PutResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setCode(StatusCode.BAD_REQUEST)
                    .setRoutingHeader(request.getHeader())
                    .build())
                .build());
        }

        RangeId rid = RangeId.of(header.getStreamId(), header.getRangeId());
        TableStore store = tableStoreCache.getTableStore(rid);
        if (null != store) {
            return store.put(request);
        } else {
            return tableStoreCache.openTableStore(scId, rid)
                .thenCompose(s -> s.put(request));
        }
    }

    @Override
    public CompletableFuture<DeleteRangeResponse> delete(DeleteRangeRequest request) {
        RoutingHeader header = request.getHeader();

        if (header.getRangeId() <= 0L) {
            return CompletableFuture.completedFuture(DeleteRangeResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setCode(StatusCode.BAD_REQUEST)
                    .setRoutingHeader(request.getHeader())
                    .build())
                .build());
        }

        RangeId rid = RangeId.of(header.getStreamId(), header.getRangeId());
        TableStore store = tableStoreCache.getTableStore(rid);
        if (null != store) {
            return store.delete(request);
        } else {
            return tableStoreCache.openTableStore(scId, rid)
                .thenCompose(s -> s.delete(request));
        }
    }

    @Override
    public CompletableFuture<TxnResponse> txn(TxnRequest request) {
        RoutingHeader header = request.getHeader();

        if (header.getRangeId() <= 0L) {
            return CompletableFuture.completedFuture(TxnResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setCode(StatusCode.BAD_REQUEST)
                    .setRoutingHeader(request.getHeader())
                    .build())
                .build());
        }

        RangeId rid = RangeId.of(header.getStreamId(), header.getRangeId());
        TableStore store = tableStoreCache.getTableStore(rid);
        if (null != store) {
            return store.txn(request);
        } else {
            return tableStoreCache.openTableStore(scId, rid)
                .thenCompose(s -> s.txn(request));
        }
    }

    @Override
    public CompletableFuture<IncrementResponse> incr(IncrementRequest request) {
        RoutingHeader header = request.getHeader();

        if (header.getRangeId() <= 0L) {
            return CompletableFuture.completedFuture(IncrementResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setCode(StatusCode.BAD_REQUEST)
                    .setRoutingHeader(request.getHeader())
                    .build())
                .build());
        }

        RangeId rid = RangeId.of(header.getStreamId(), header.getRangeId());
        TableStore store = tableStoreCache.getTableStore(rid);
        if (null != store) {
            return store.incr(request);
        } else {
            return tableStoreCache.openTableStore(scId, rid)
                .thenCompose(s -> s.incr(request));
        }
    }

}
