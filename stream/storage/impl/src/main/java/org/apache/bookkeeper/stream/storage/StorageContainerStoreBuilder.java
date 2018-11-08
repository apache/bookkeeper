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

package org.apache.bookkeeper.stream.storage;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.util.function.Supplier;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.bookkeeper.stream.storage.api.StorageContainerStore;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManagerFactory;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.StorageContainerStoreImpl;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerPlacementPolicyImpl;
import org.apache.bookkeeper.stream.storage.impl.service.RangeStoreContainerServiceFactoryImpl;
import org.apache.bookkeeper.stream.storage.impl.service.RangeStoreServiceFactoryImpl;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;

/**
 * Builder to build the storage component.
 */
public final class StorageContainerStoreBuilder {

    public static StorageContainerStoreBuilder newBuilder() {
        return new StorageContainerStoreBuilder();
    }

    private StatsLogger statsLogger = NullStatsLogger.INSTANCE;
    private StorageConfiguration storeConf = null;
    private StorageResources storeResources = null;
    private StorageContainerManagerFactory scmFactory = null;
    private StorageContainerPlacementPolicy.Factory placementPolicyFactory = () ->
        StorageContainerPlacementPolicyImpl.of(1024);
    private MVCCStoreFactory mvccStoreFactory = null;
    private URI defaultBackendUri = null;
    private Supplier<StorageServerClientManager> clientManagerSupplier;

    private StorageContainerStoreBuilder() {
    }

    /**
     * Build the range store with the provided <tt>placementPolicyFactory</tt>.
     *
     * @param placementPolicyFactory placement policy factor to create placement policies.
     * @return range store builder.
     */
    public StorageContainerStoreBuilder withStorageContainerPlacementPolicyFactory(
        StorageContainerPlacementPolicy.Factory placementPolicyFactory) {
        this.placementPolicyFactory = placementPolicyFactory;
        return this;
    }

    /**
     * Build the range store with the provided {@link StatsLogger}.
     *
     * @param statsLogger stats logger for collecting stats.
     * @return range store builder;
     */
    public StorageContainerStoreBuilder withStatsLogger(StatsLogger statsLogger) {
        if (null == statsLogger) {
            return this;
        }
        this.statsLogger = statsLogger;
        return this;
    }

    /**
     * Build the range store with provided {@link StorageConfiguration}.
     *
     * @param storeConf storage configuration
     * @return range store builder
     */
    public StorageContainerStoreBuilder withStorageConfiguration(StorageConfiguration storeConf) {
        this.storeConf = storeConf;
        return this;
    }

    /**
     * Build the range store with provided {@link StorageContainerManagerFactory}.
     *
     * @param scmFactory storage container manager factory.
     * @return range store builder
     */
    public StorageContainerStoreBuilder withStorageContainerManagerFactory(StorageContainerManagerFactory scmFactory) {
        this.scmFactory = scmFactory;
        return this;
    }

    /**
     * Build the range store with provided {@link StorageResources}.
     *
     * @param resources storage resources.
     * @return range store builder.
     */
    public StorageContainerStoreBuilder withStorageResources(StorageResources resources) {
        this.storeResources = resources;
        return this;
    }

    /**
     * Build the range store with provided {@link MVCCStoreFactory}.
     *
     * @param storeFactory factory to create range stores.
     * @return range store builder.
     */
    public StorageContainerStoreBuilder withRangeStoreFactory(MVCCStoreFactory storeFactory) {
        this.mvccStoreFactory = storeFactory;
        return this;
    }

    /**
     * Backend uri for storing table ranges.
     *
     * @param uri uri for storing table ranges.
     * @return range store builder.
     */
    public StorageContainerStoreBuilder withDefaultBackendUri(URI uri) {
        this.defaultBackendUri = uri;
        return this;
    }

    /**
     * Supplier to provide client manager for proxying requests.
     *
     * @param clientManagerSupplier client manager supplier
     * @return storage container store builder
     */
    public StorageContainerStoreBuilder withStorageServerClientManager(
        Supplier<StorageServerClientManager> clientManagerSupplier) {
        this.clientManagerSupplier = clientManagerSupplier;
        return this;
    }

    public StorageContainerStore build() {
        checkNotNull(scmFactory, "StorageContainerManagerFactory is not provided");
        checkNotNull(storeConf, "StorageConfiguration is not provided");
        checkNotNull(mvccStoreFactory, "MVCCStoreFactory is not provided");
        checkNotNull(defaultBackendUri, "Default backend uri is not provided");
        checkNotNull(placementPolicyFactory, "Storage Container Placement Policy Factory is not provided");
        checkNotNull(clientManagerSupplier, "Storage server client manager is not provided");

        RangeStoreServiceFactoryImpl serviceFactory = new RangeStoreServiceFactoryImpl(
            storeConf,
            placementPolicyFactory.newPlacementPolicy(),
            storeResources.scheduler(),
            mvccStoreFactory,
            clientManagerSupplier.get());

        RangeStoreContainerServiceFactoryImpl containerServiceFactory =
            new RangeStoreContainerServiceFactoryImpl(serviceFactory);

        return new StorageContainerStoreImpl(
            storeConf,
            scmFactory,
            containerServiceFactory,
            clientManagerSupplier.get(),
            statsLogger);
    }

}
