/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.distributedlog.stream.storage.impl;

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.stats.StatsLogger;
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
import org.apache.distributedlog.stream.storage.api.RangeStore;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainer;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainerManager;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainerManagerFactory;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainerRoutingService;
import org.apache.distributedlog.stream.storage.conf.StorageConfiguration;
import org.apache.distributedlog.stream.storage.impl.sc.DefaultStorageContainerFactory;
import org.apache.distributedlog.stream.storage.impl.sc.StorageContainerPlacementPolicyImpl;
import org.apache.distributedlog.stream.storage.impl.sc.StorageContainerRegistryImpl;
import org.apache.distributedlog.stream.storage.impl.store.RangeStoreFactory;

/**
 * KeyRange Service.
 */
public class RangeStoreImpl
  extends AbstractLifecycleComponent<StorageConfiguration>
  implements RangeStore {

  private final Resource<OrderedScheduler> schedulerResource;
  private final OrderedScheduler scheduler;
  private final StorageContainerManagerFactory scmFactory;
  private final StorageContainerRegistryImpl scRegistry;
  private final StorageContainerManager scManager;
  private final RangeStoreFactory storeFactory;

  public RangeStoreImpl(StorageConfiguration conf,
                        Resource<OrderedScheduler> schedulerResource,
                        StorageContainerManagerFactory factory,
                        RangeStoreFactory rangeStoreFactory,
                        int numStorageContainers,
                        StatsLogger statsLogger) {
    super("range-service", conf, statsLogger);
    this.schedulerResource = schedulerResource;
    this.scheduler = SharedResourceManager.shared().get(schedulerResource);
    this.scmFactory = factory;
    StorageContainerPlacementPolicy placementPolicy =
      StorageContainerPlacementPolicyImpl.of(numStorageContainers);
    this.storeFactory = rangeStoreFactory;
    this.scRegistry = new StorageContainerRegistryImpl(
      new DefaultStorageContainerFactory(
        conf,
        placementPolicy,
        scheduler,
        storeFactory),
      scheduler);
    this.scManager = scmFactory.create(numStorageContainers, conf, scRegistry);
  }

  @Override
  public ScheduledExecutorService chooseExecutor(long key) {
    return this.scheduler.chooseThread(key);
  }

  @VisibleForTesting
  StorageContainerRegistryImpl getRegistry() {
    return this.scRegistry;
  }

  @Override
  public StorageContainerRoutingService getRoutingService() {
    return this.scManager;
  }

  //
  // Lifecycle management
  //

  @Override
  protected void doStart() {
    this.scManager.start();
  }

  @Override
  protected void doStop() {
    this.scManager.stop();
    this.scRegistry.close();
  }

  @Override
  protected void doClose() throws IOException {
    this.scManager.close();
    // stop the core scheduler
    SharedResourceManager.shared().release(
      schedulerResource,
      scheduler);
  }

  private StorageContainer getStorageContainer(long scId) {
    return scRegistry.getStorageContainer(scId);
  }

  //
  // Root Range Service
  //

  @Override
  public CompletableFuture<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest request) {
    return getStorageContainer(ROOT_STORAGE_CONTAINER_ID).createNamespace(request);
  }

  @Override
  public CompletableFuture<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest request) {
    return getStorageContainer(ROOT_STORAGE_CONTAINER_ID).deleteNamespace(request);
  }

  @Override
  public CompletableFuture<GetNamespaceResponse> getNamespace(GetNamespaceRequest request) {
    return getStorageContainer(ROOT_STORAGE_CONTAINER_ID).getNamespace(request);
  }

  @Override
  public CompletableFuture<CreateStreamResponse> createStream(CreateStreamRequest request) {
    return getStorageContainer(ROOT_STORAGE_CONTAINER_ID).createStream(request);
  }

  @Override
  public CompletableFuture<DeleteStreamResponse> deleteStream(DeleteStreamRequest request) {
    return getStorageContainer(ROOT_STORAGE_CONTAINER_ID).deleteStream(request);
  }

  @Override
  public CompletableFuture<GetStreamResponse> getStream(GetStreamRequest request) {
    return getStorageContainer(ROOT_STORAGE_CONTAINER_ID).getStream(request);
  }

  //
  // Stream Meta Range Service
  //

  @Override
  public CompletableFuture<StorageContainerResponse> getActiveRanges(StorageContainerRequest request) {
    return getStorageContainer(request.getScId()).getActiveRanges(request);
  }

}
