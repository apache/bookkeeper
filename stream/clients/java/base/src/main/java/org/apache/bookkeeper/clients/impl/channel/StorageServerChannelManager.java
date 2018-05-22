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

package org.apache.bookkeeper.clients.impl.channel;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.stream.proto.common.Endpoint;

/**
 * A manager manages channels to range servers.
 */
@Slf4j
public class StorageServerChannelManager implements AutoCloseable {

    private final ReentrantReadWriteLock lock;
    private boolean closed = false;
    private final ConcurrentMap<Endpoint, StorageServerChannel> channels;
    private final Function<Endpoint, StorageServerChannel> channelFactory;

    public StorageServerChannelManager(StorageClientSettings settings) {
        this(StorageServerChannel.factory(settings));
    }

    @VisibleForTesting
    public StorageServerChannelManager(Function<Endpoint, StorageServerChannel> channelFactory) {
        this.channels = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.channelFactory = channelFactory;
    }

    @VisibleForTesting
    int getNumChannels() {
        return channels.size();
    }

    @VisibleForTesting
    boolean contains(Endpoint endpoint) {
        lock.readLock().lock();
        try {
            return channels.containsKey(endpoint);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean addStorageServer(Endpoint endpoint, StorageServerChannel channel) {
        lock.readLock().lock();
        try {
            if (closed) {
                log.warn("Skip adding channel {} of range server {} since the channel manager is already closed",
                    channel, endpoint);
                channel.close();
                return false;
            }

            StorageServerChannel oldChannel = channels.putIfAbsent(endpoint, channel);
            if (null != oldChannel) {
                log.debug("KeyRange server ({}) already existed in the channel manager.");
                channel.close();
                return false;
            } else {
                log.info("Added range server ({}) into the channel manager.", endpoint);
                return true;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public StorageServerChannel getOrCreateChannel(Endpoint endpoint) {
        StorageServerChannel channel = getChannel(endpoint);
        if (null != channel) {
            return channel;
        }
        // no channel exists
        StorageServerChannel newChannel = channelFactory.apply(endpoint);
        addStorageServer(endpoint, newChannel);
        return getChannel(endpoint);
    }

    @Nullable
    public StorageServerChannel getChannel(Endpoint endpoint) {
        lock.readLock().lock();
        try {
            return channels.get(endpoint);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Nullable
    public StorageServerChannel removeChannel(Endpoint endpoint, StorageServerChannel channel) {
        lock.readLock().lock();
        try {
            if (closed) {
                log.warn("Skip removing channel {} of range server {} since the channel manager is already closed",
                    channel, endpoint);
                return null;
            }

            StorageServerChannel channelRemoved;
            if (null == channel) {
                channelRemoved = channels.remove(endpoint);
            } else {
                if (channels.remove(endpoint, channel)) {
                    channelRemoved = channel;
                } else {
                    channelRemoved = null;
                }
            }
            if (null == channelRemoved) {
                log.debug("No channel associated with endpoint {} to be removed.");
            } else {
                log.info("Removed channel {} for range server {} successfully",
                    channelRemoved, endpoint);
            }
            if (null != channelRemoved) {
                channelRemoved.close();
            }
            return channelRemoved;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
        // close the channels
        channels.values().forEach(StorageServerChannel::close);
        channels.clear();
    }
}
