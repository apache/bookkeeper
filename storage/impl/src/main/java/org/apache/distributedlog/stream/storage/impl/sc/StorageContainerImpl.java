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

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.CONTAINER_META_RANGE_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.CONTAINER_META_STREAM_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_RANGE_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_STREAM_ID;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
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
import org.apache.distributedlog.stream.proto.storage.StorageContainerResponse;
import org.apache.distributedlog.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.distributedlog.stream.storage.api.metadata.MetaRangeStore;
import org.apache.distributedlog.stream.storage.api.metadata.RootRangeStore;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainer;
import org.apache.distributedlog.stream.storage.conf.StorageConfiguration;
import org.apache.distributedlog.stream.storage.impl.metadata.MetaRangeStoreImpl;
import org.apache.distributedlog.stream.storage.impl.metadata.RootRangeStoreImpl;
import org.apache.distributedlog.stream.storage.impl.store.RangeStoreFactory;

/**
 * The default implementation of {@link StorageContainer}.
 */
@Slf4j
public class StorageContainerImpl
    implements StorageContainer {

  private final long scId;
  private final ScheduledExecutorService scExecutor;
  private final StorageConfiguration storageConf;

  // storage container placement policy
  private final StorageContainerPlacementPolicy placementPolicy;
  // store container that used for fail requests.
  private final StorageContainer failRequestStorageContainer;
  // store factory
  private final RangeStoreFactory storeFactory;
  // storage container
  private MetaRangeStore mgStore;
  // root range
  private RootRangeStore rootRange;

  public StorageContainerImpl(StorageConfiguration storageConf,
                              long scId,
                              StorageContainerPlacementPolicy rangePlacementPolicy,
                              OrderedScheduler scheduler,
                              RangeStoreFactory storeFactory) {
    this.scId = scId;
    this.scExecutor = scheduler.chooseThread(scId);
    this.storageConf = storageConf;
    this.failRequestStorageContainer = FailRequestStorageContainer.of(scheduler);
    this.storeFactory = storeFactory;
    this.rootRange = failRequestStorageContainer;
    this.mgStore = failRequestStorageContainer;
    this.placementPolicy = rangePlacementPolicy;
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
      rootRange = new RootRangeStoreImpl(
          store,
          placementPolicy,
          scExecutor);
      return null;
    });
  }

  private CompletableFuture<Void> startMetaRangeStore(long scId) {
    return storeFactory.openStore(
        scId,
        CONTAINER_META_STREAM_ID,
        CONTAINER_META_RANGE_ID
    ).thenApply(store -> {
      mgStore = new MetaRangeStoreImpl(
          store,
          placementPolicy,
          scExecutor);
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

}
