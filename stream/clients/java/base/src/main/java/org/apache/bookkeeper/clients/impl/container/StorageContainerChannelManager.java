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

package org.apache.bookkeeper.clients.impl.container;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannelManager;
import org.apache.bookkeeper.clients.impl.internal.api.LocationClient;
import org.apache.bookkeeper.common.util.OrderedScheduler;

/**
 * A manager manages channels/clients to storage containers.
 */
public class StorageContainerChannelManager implements AutoCloseable {

    private final StorageContainerChannelFactory factory;
    private final ConcurrentMap<Long, StorageContainerChannel> scChannels;

    public StorageContainerChannelManager(StorageServerChannelManager channelManager,
                                          LocationClient locationClient,
                                          OrderedScheduler scheduler) {
        this((scId) -> new StorageContainerChannel(
            scId,
            channelManager,
            locationClient,
            scheduler.chooseThread(scId)));
    }

    @VisibleForTesting
    StorageContainerChannelManager(StorageContainerChannelFactory factory) {
        this.factory = factory;
        this.scChannels = Maps.newConcurrentMap();
    }

    @VisibleForTesting
    int getNumChannels() {
        return scChannels.size();
    }

    /**
     * Retrieve the storage container channel for storage container {@code scId}.
     *
     * <p>This call will create the storage container channel if it doesn't exist before.
     *
     * @param scId storage container id.
     * @return storage container channel.
     */
    public StorageContainerChannel getOrCreate(long scId) {
        StorageContainerChannel scChannel = scChannels.get(scId);
        if (null == scChannel) {
            StorageContainerChannel newChannel = factory.createStorageContainerChannel(scId);
            StorageContainerChannel oldChannel = scChannels.putIfAbsent(scId, newChannel);
            if (null == oldChannel) {
                scChannel = newChannel;
            } else {
                scChannel = oldChannel;
            }
        }
        return scChannel;
    }

    /**
     * Remove the storage container channel from the channels map.
     *
     * @param scId storage container channel id.
     * @return the removed storage container channel.
     */
    public StorageContainerChannel remove(long scId) {
        return scChannels.remove(scId);
    }

    @Override
    public void close() throws Exception {
        scChannels.clear();
    }
}
