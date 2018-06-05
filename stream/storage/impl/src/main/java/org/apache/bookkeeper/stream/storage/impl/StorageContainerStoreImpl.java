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

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_STORAGE_CONTAINER_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.SC_ID_KEY;

import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import java.io.IOException;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.grpc.netty.LongBinaryMarshaller;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.storage.api.StorageContainerStore;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManager;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManagerFactory;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerRoutingService;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerServiceFactory;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.sc.DefaultStorageContainerFactory;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerRegistryImpl;

/**
 * KeyRange Service.
 */
public class StorageContainerStoreImpl
    extends AbstractLifecycleComponent<StorageConfiguration>
    implements StorageContainerStore {

    private final StorageContainerManagerFactory scmFactory;
    private final StorageContainerRegistryImpl scRegistry;
    private final StorageContainerManager scManager;
    private final StorageContainerServiceFactory serviceFactory;
    private final Metadata.Key<Long> scIdKey;

    public StorageContainerStoreImpl(StorageConfiguration conf,
                                     StorageContainerManagerFactory managerFactory,
                                     StorageContainerServiceFactory serviceFactory,
                                     StatsLogger statsLogger) {
        super("range-service", conf, statsLogger);
        this.scmFactory = managerFactory;
        this.scRegistry = new StorageContainerRegistryImpl(
            new DefaultStorageContainerFactory(serviceFactory));
        this.scManager = scmFactory.create(conf, scRegistry);
        this.serviceFactory = serviceFactory;
        this.scIdKey = Metadata.Key.of(
            SC_ID_KEY,
            LongBinaryMarshaller.of());
    }

    @Override
    public StorageContainerRegistryImpl getRegistry() {
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
        this.serviceFactory.close();
    }

    StorageContainer getStorageContainer(long scId) {
        return scRegistry.getStorageContainer(scId);
    }

    //
    // Utils for proxies
    //

    @Override
    public Channel findChannel(ServerCall<?, ?> serverCall, Metadata headers) {
        Long scId = headers.get(scIdKey);
        if (null == scId) {
            // use the invalid storage container id, so it will fail the request.
            scId = INVALID_STORAGE_CONTAINER_ID;
        }
        return getStorageContainer(scId).getChannel();
    }
}
