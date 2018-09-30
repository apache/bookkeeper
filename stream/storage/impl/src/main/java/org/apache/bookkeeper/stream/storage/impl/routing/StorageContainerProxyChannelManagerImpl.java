/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.stream.storage.impl.routing;

import io.grpc.Channel;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.impl.internal.api.StorageServerClientManager;

/**
 * Default implementation of {@link StorageContainerProxyChannelManager}.
 */
public class StorageContainerProxyChannelManagerImpl implements StorageContainerProxyChannelManager {

    // we can ideally just talk to the location service directly.
    // however currently storage container is separated from actual services to make a clean interface
    // so for now proxy channel manager will be acting as a client to talk to its local server to
    // get location related information
    private final StorageServerClientManager ssClientManager;

    public StorageContainerProxyChannelManagerImpl(StorageServerClientManager clientManager) {
        this.ssClientManager = clientManager;
    }

    @Override
    public Channel getStorageContainerChannel(long scId) {
        StorageContainerChannel channel = ssClientManager.getStorageContainerChannel(scId);
        // this will trigger creating the channel for the first time
        CompletableFuture<StorageServerChannel> serverChannelFuture = channel.getStorageContainerChannelFuture();
        if (null != serverChannelFuture && serverChannelFuture.isDone()) {
            StorageServerChannel serverChannel = serverChannelFuture.join();
            if (serverChannel != null) {
                return serverChannel.getGrpcChannel();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

}
