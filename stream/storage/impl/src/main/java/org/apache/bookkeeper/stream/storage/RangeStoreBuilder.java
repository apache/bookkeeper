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
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.bookkeeper.stream.storage.api.RangeStore;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManagerFactory;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.RangeStoreImpl;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerPlacementPolicyImpl;
import org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactory;

/**
 * Builder to build the storage component.
 */
public final class RangeStoreBuilder {

    public static RangeStoreBuilder newBuilder() {
        return new RangeStoreBuilder();
    }

    private StatsLogger statsLogger = NullStatsLogger.INSTANCE;
    private StorageConfiguration storeConf = null;
    private StorageResources storeResources = null;
    private StorageContainerManagerFactory scmFactory = null;
    private StorageContainerPlacementPolicy.Factory placementPolicyFactory = () ->
        StorageContainerPlacementPolicyImpl.of(1024);
    private MVCCStoreFactory mvccStoreFactory = null;
    private URI defaultBackendUri = null;

    private RangeStoreBuilder() {
    }

    /**
     * Build the range store with the provided {@code numStorageContainers}.
     *
     * @param numStorageContainers number of the storage containers.
     * @return range store builder
     */
    public RangeStoreBuilder withNumStorageContainers(int numStorageContainers) {
        this.placementPolicyFactory = () -> StorageContainerPlacementPolicyImpl.of(numStorageContainers);
        return this;
    }

    /**
     * Build the range store with the provided <tt>placementPolicyFactory</tt>.
     *
     * @param placementPolicyFactory placement policy factor to create placement policies.
     * @return range store builder.
     */
    public RangeStoreBuilder withStorageContainerPlacementPolicyFactory(
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
    public RangeStoreBuilder withStatsLogger(StatsLogger statsLogger) {
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
    public RangeStoreBuilder withStorageConfiguration(StorageConfiguration storeConf) {
        this.storeConf = storeConf;
        return this;
    }

    /**
     * Build the range store with provided {@link StorageContainerManagerFactory}.
     *
     * @param scmFactory storage container manager factory.
     * @return range store builder
     */
    public RangeStoreBuilder withStorageContainerManagerFactory(StorageContainerManagerFactory scmFactory) {
        this.scmFactory = scmFactory;
        return this;
    }

    /**
     * Build the range store with provided {@link StorageResources}.
     *
     * @param resources storage resources.
     * @return range store builder.
     */
    public RangeStoreBuilder withStorageResources(StorageResources resources) {
        this.storeResources = resources;
        return this;
    }

    /**
     * Build the range store with provided {@link MVCCStoreFactory}.
     *
     * @param storeFactory factory to create range stores.
     * @return range store builder.
     */
    public RangeStoreBuilder withRangeStoreFactory(MVCCStoreFactory storeFactory) {
        this.mvccStoreFactory = storeFactory;
        return this;
    }

    /**
     * Backend uri for storing table ranges.
     *
     * @param uri uri for storing table ranges.
     * @return range store builder.
     */
    public RangeStoreBuilder withDefaultBackendUri(URI uri) {
        this.defaultBackendUri = uri;
        return this;
    }

    public RangeStore build() {
        checkNotNull(scmFactory, "StorageContainerManagerFactory is not provided");
        checkNotNull(storeConf, "StorageConfiguration is not provided");
        checkNotNull(mvccStoreFactory, "MVCCStoreFactory is not provided");
        checkNotNull(defaultBackendUri, "Default backend uri is not provided");
        checkNotNull(placementPolicyFactory, "Storage Container Placement Policy Factory is not provided");

        return new RangeStoreImpl(
            storeConf,
            storeResources.scheduler(),
            scmFactory,
            mvccStoreFactory,
            defaultBackendUri,
            placementPolicyFactory,
            statsLogger);
    }

}
