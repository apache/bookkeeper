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

package org.apache.bookkeeper.stream.storage.impl;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerResponse;
import org.apache.bookkeeper.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.bookkeeper.stream.storage.api.RangeStore;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManager;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManagerFactory;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerRoutingService;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.sc.DefaultStorageContainerFactory;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerRegistryImpl;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;

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
    private final MVCCStoreFactory storeFactory;

    public RangeStoreImpl(StorageConfiguration conf,
                          Resource<OrderedScheduler> schedulerResource,
                          StorageContainerManagerFactory factory,
                          MVCCStoreFactory mvccStoreFactory,
                          URI defaultBackendUri,
                          StorageContainerPlacementPolicy.Factory placementPolicyFactory,
                          StatsLogger statsLogger) {
        super("range-service", conf, statsLogger);
        this.schedulerResource = schedulerResource;
        this.scheduler = SharedResourceManager.shared().get(schedulerResource);
        this.scmFactory = factory;
        this.storeFactory = mvccStoreFactory;
        StorageContainerPlacementPolicy placementPolicy = placementPolicyFactory.newPlacementPolicy();
        this.scRegistry = new StorageContainerRegistryImpl(
            new DefaultStorageContainerFactory(
                conf,
                placementPolicy,
                scheduler,
                storeFactory,
                defaultBackendUri),
            scheduler);
        this.scManager = scmFactory.create(conf, scRegistry);
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

    //
    // Table Service
    //

    @Override
    public CompletableFuture<StorageContainerResponse> range(StorageContainerRequest request) {
        return getStorageContainer(request.getScId()).range(request);
    }

    @Override
    public CompletableFuture<StorageContainerResponse> put(StorageContainerRequest request) {
        return getStorageContainer(request.getScId()).put(request);
    }

    @Override
    public CompletableFuture<StorageContainerResponse> delete(StorageContainerRequest request) {
        return getStorageContainer(request.getScId()).delete(request);
    }

    @Override
    public CompletableFuture<StorageContainerResponse> txn(StorageContainerRequest request) {
        return getStorageContainer(request.getScId()).txn(request);
    }

    @Override
    public CompletableFuture<StorageContainerResponse> incr(StorageContainerRequest request) {
        return getStorageContainer(request.getScId()).incr(request);
    }
}
