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
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.RID_METADATA_KEY;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.RK_METADATA_KEY;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.SCID_METADATA_KEY;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.SID_METADATA_KEY;

import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import java.io.IOException;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.storage.api.StorageContainerStore;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManager;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerManagerFactory;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerRoutingService;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerServiceFactory;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.bookkeeper.stream.storage.impl.routing.RangeRoutingTable;
import org.apache.bookkeeper.stream.storage.impl.routing.RangeRoutingTableImpl;
import org.apache.bookkeeper.stream.storage.impl.routing.StorageContainerProxyChannelManager;
import org.apache.bookkeeper.stream.storage.impl.routing.StorageContainerProxyChannelManagerImpl;
import org.apache.bookkeeper.stream.storage.impl.sc.DefaultStorageContainerFactory;
import org.apache.bookkeeper.stream.storage.impl.sc.StorageContainerRegistryImpl;

/**
 * Storage Container Store manages the containers and routes the requests accordingly.
 */
public class StorageContainerStoreImpl
    extends AbstractLifecycleComponent<StorageConfiguration>
    implements StorageContainerStore {

    private final StorageContainerManagerFactory scmFactory;
    private final StorageContainerRegistryImpl scRegistry;
    private final StorageContainerManager scManager;
    private final StorageContainerServiceFactory serviceFactory;
    private final StorageServerClientManager ssClientManager;
    private final RangeRoutingTable routingTable;
    private final StorageContainerProxyChannelManager proxyChannelManager;

    public StorageContainerStoreImpl(StorageConfiguration conf,
                                     StorageContainerManagerFactory managerFactory,
                                     StorageContainerServiceFactory serviceFactory,
                                     StorageServerClientManager ssClientManager,
                                     StatsLogger statsLogger) {
        super("range-service", conf, statsLogger);
        this.scmFactory = managerFactory;
        this.scRegistry = new StorageContainerRegistryImpl(
            new DefaultStorageContainerFactory(serviceFactory));
        this.scManager = scmFactory.create(conf, scRegistry);
        this.serviceFactory = serviceFactory;
        this.ssClientManager = ssClientManager;
        if (ssClientManager == null) {
            this.proxyChannelManager = null;
            this.routingTable = null;
        } else {
            this.proxyChannelManager = new StorageContainerProxyChannelManagerImpl(ssClientManager);
            this.routingTable = new RangeRoutingTableImpl(ssClientManager);
        }
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
        // it doesn't have to be blocked at waiting closing proxy channels to be completed.
        if (null != ssClientManager) {
            this.ssClientManager.closeAsync();
        }
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

    StorageContainer getStorageContainer(long scId, StorageContainer defaultContainer) {
        return scRegistry.getStorageContainer(scId, defaultContainer);
    }

    //
    // Utils for proxies
    //

    @Override
    public Channel findChannel(ServerCall<?, ?> serverCall, Metadata headers) {
        Long scId = headers.get(SCID_METADATA_KEY);
        if (null != scId) {
            // if a request is sent directly to a container, then find the container
            StorageContainer container = getStorageContainer(scId, null);
            if (null != container) {
                return container.getChannel();
            } else {
                if (scId == 0L && null != proxyChannelManager) {
                    // root container, we attempt to always proxy the requests for root container
                    Channel channel = proxyChannelManager.getStorageContainerChannel(0L);
                    if (null != channel) {
                        return channel;
                    } else {
                        // no channel found to proxy the request, fail the request with 404 container
                        return getStorageContainer(INVALID_STORAGE_CONTAINER_ID).getChannel();
                    }
                } else {
                    // no container is found and the scId is not the root container
                    // then fail the request with 404 container
                    return getStorageContainer(INVALID_STORAGE_CONTAINER_ID).getChannel();
                }
            }
        } else {
            // if a request is not sent directly to a container, then check if
            // streamId + routingKey is attached in the header. if so, figure out
            // which the range id and storage container id to route the request.
            byte[] routingKey = headers.get(RK_METADATA_KEY);
            Long streamId = headers.get(SID_METADATA_KEY);
            if (null != routingKey && null != streamId && null != routingTable && null != proxyChannelManager) {
                RangeProperties rangeProps = routingTable.getRange(streamId, routingKey);
                if (null != rangeProps) {
                    long containerId = rangeProps.getStorageContainerId();
                    long rangeId = rangeProps.getRangeId();
                    // if we find the routing information, we can update the headers
                    headers.put(SCID_METADATA_KEY, containerId);
                    headers.put(RID_METADATA_KEY, rangeId);
                    // if we find the container id, we can check whether the container is owned locally.
                    // if so, forward the request to the container.
                    StorageContainer container = getStorageContainer(containerId, null);
                    if (null == container) {
                        // the container doesn't belong to here, then find the channel to forward the request
                        Channel channel = proxyChannelManager.getStorageContainerChannel(containerId);
                        if (null != channel) {
                            return channel;
                        }
                    } else {
                        // we found the container exists in the registry to serve the request
                        return container.getChannel();
                    }
                }
            }
            // there is no storage container id or routing key, then fail the request and ask client to retry.
            return getStorageContainer(INVALID_STORAGE_CONTAINER_ID).getChannel();
        }
    }
}
