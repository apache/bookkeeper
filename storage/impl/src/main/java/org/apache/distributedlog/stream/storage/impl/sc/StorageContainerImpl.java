/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.stream.storage.impl.sc;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.CONTAINER_META_RANGE_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.CONTAINER_META_STREAM_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_RANGE_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_STREAM_ID;

import com.google.common.collect.Lists;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.IncrementRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.PutRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.RangeRequest;
import org.apache.distributedlog.stream.proto.kv.rpc.RoutingHeader;
import org.apache.distributedlog.stream.proto.kv.rpc.TxnRequest;
import org.apache.distributedlog.stream.proto.storage.CreateNamespaceRequest;
import org.apache.distributedlog.stream.proto.storage.CreateNamespaceResponse;
import org.apache.distributedlog.stream.proto.storage.CreateStreamRequest;
import org.apache.distributedlog.stream.proto.storage.CreateStreamResponse;
import org.apache.distributedlog.stream.proto.storage.DeleteNamespaceRequest;
import org.apache.distributedlog.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.distributedlog.stream.proto.storage.DeleteStreamRequest;
import org.apache.distributedlog.stream.proto.storage.DeleteStreamResponse;
import org.apache.distributedlog.stream.proto.storage.GetNamespaceRequest;
import org.apache.distributedlog.stream.proto.storage.GetNamespaceResponse;
import org.apache.distributedlog.stream.proto.storage.GetStreamRequest;
import org.apache.distributedlog.stream.proto.storage.GetStreamResponse;
import org.apache.distributedlog.stream.proto.storage.StorageContainerRequest;
import org.apache.distributedlog.stream.proto.storage.StorageContainerRequest.Type;
import org.apache.distributedlog.stream.proto.storage.StorageContainerResponse;
import org.apache.distributedlog.stream.protocol.RangeId;
import org.apache.distributedlog.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.distributedlog.stream.storage.api.kv.TableStore;
import org.apache.distributedlog.stream.storage.api.metadata.MetaRangeStore;
import org.apache.distributedlog.stream.storage.api.metadata.RootRangeStore;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainer;
import org.apache.distributedlog.stream.storage.conf.StorageConfiguration;
import org.apache.distributedlog.stream.storage.impl.kv.TableStoreCache;
import org.apache.distributedlog.stream.storage.impl.kv.TableStoreFactory;
import org.apache.distributedlog.stream.storage.impl.kv.TableStoreImpl;
import org.apache.distributedlog.stream.storage.impl.metadata.MetaRangeStoreFactory;
import org.apache.distributedlog.stream.storage.impl.metadata.MetaRangeStoreImpl;
import org.apache.distributedlog.stream.storage.impl.metadata.RootRangeStoreFactory;
import org.apache.distributedlog.stream.storage.impl.metadata.RootRangeStoreImpl;
import org.apache.distributedlog.stream.storage.impl.store.MVCCStoreFactory;

/**
 * The default implementation of {@link StorageContainer}.
 */
@Slf4j
public class StorageContainerImpl
    implements StorageContainer {

  private final long scId;

  // store container that used for fail requests.
  private final StorageContainer failRequestStorageContainer;
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

  public StorageContainerImpl(StorageConfiguration storageConf,
                              long scId,
                              StorageContainerPlacementPolicy rangePlacementPolicy,
                              OrderedScheduler scheduler,
                              MVCCStoreFactory storeFactory,
                              URI defaultBackendUri) {
      this(
          scId,
          scheduler,
          storeFactory,
          store -> new RootRangeStoreImpl(defaultBackendUri, store, rangePlacementPolicy, scheduler.chooseThread(scId)),
          store -> new MetaRangeStoreImpl(store, rangePlacementPolicy, scheduler.chooseThread(scId)),
          store -> new TableStoreImpl(store));
  }

  public StorageContainerImpl(long scId,
                              OrderedScheduler scheduler,
                              MVCCStoreFactory storeFactory,
                              RootRangeStoreFactory rrStoreFactory,
                              MetaRangeStoreFactory mrStoreFactory,
                              TableStoreFactory tableStoreFactory) {
    this.scId = scId;
    this.failRequestStorageContainer = FailRequestStorageContainer.of(scheduler);
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

  @Override
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
    log.info("Starting storage container ({}) ...", getId());

    List<CompletableFuture<Void>> futures = Lists.newArrayList(
        startRootRangeStore(),
        startMetaRangeStore(scId));

    return FutureUtils.collect(futures).thenApply(ignored -> {
      log.info("Successfully started storage container ({}).", getId());
      return null;
    });
  }

  @Override
  public CompletableFuture<Void> stop() {
    log.info("Stopping storage container ({}) ...", getId());

    return storeFactory.closeStores(scId).thenApply(ignored -> {
      log.info("Successfully stopped storage container ({}).", getId());
      return null;
    });
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
  public CompletableFuture<StorageContainerResponse> getActiveRanges(StorageContainerRequest request) {
    return mgStore.getActiveRanges(request);
  }

  //
  // Table API
  //


  @Override
  public CompletableFuture<StorageContainerResponse> range(StorageContainerRequest request) {
    checkArgument(Type.KV_RANGE == request.getType());

    long scId = request.getScId();
    RangeRequest rr = request.getKvRangeReq();
    RoutingHeader header = rr.getHeader();

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
  public CompletableFuture<StorageContainerResponse> put(StorageContainerRequest request) {
    checkArgument(Type.KV_PUT == request.getType());

    long scId = request.getScId();
    PutRequest rr = request.getKvPutReq();
    RoutingHeader header = rr.getHeader();

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
  public CompletableFuture<StorageContainerResponse> delete(StorageContainerRequest request) {
    checkArgument(Type.KV_DELETE == request.getType());

    long scId = request.getScId();
    DeleteRangeRequest rr = request.getKvDeleteReq();
    RoutingHeader header = rr.getHeader();

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
  public CompletableFuture<StorageContainerResponse> txn(StorageContainerRequest request) {
    checkArgument(Type.KV_TXN == request.getType());

    long scId = request.getScId();
    TxnRequest rr = request.getKvTxnReq();
    RoutingHeader header = rr.getHeader();

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
  public CompletableFuture<StorageContainerResponse> incr(StorageContainerRequest request) {
    checkArgument(Type.KV_INCREMENT == request.getType());

    long scId = request.getScId();
    IncrementRequest ir = request.getKvIncrReq();
    RoutingHeader header = ir.getHeader();

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
