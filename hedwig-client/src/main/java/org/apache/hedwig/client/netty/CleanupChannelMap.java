/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.client.netty;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanupChannelMap<T> {

    private static Logger logger = LoggerFactory.getLogger(CleanupChannelMap.class);
    
    private final ConcurrentHashMap<T, HChannel> channels;

    // Boolean indicating if the channel map is closed or not.
    protected boolean closed = false;
    protected final ReentrantReadWriteLock closedLock =
        new ReentrantReadWriteLock();

    public CleanupChannelMap() {
        channels = new ConcurrentHashMap<T, HChannel>();
    }

    /**
     * Add channel to the map. If an old channel has been bound
     * to <code>key</code>, the <code>channel</code> would be
     * closed immediately and the old channel is returned. Otherwise,
     * the <code>channel</code> is put in the map for future usage.
     *
     * If the channel map has been closed, the channel would be closed
     * immediately.
     *
     * @param key
     *            Key
     * @param channel
     *            Channel
     * @return the channel instance to use.
     */
    public HChannel addChannel(T key, HChannel channel) {
        this.closedLock.readLock().lock();
        try {
            if (closed) {
                channel.close();
                return channel;
            }
            HChannel oldChannel = channels.putIfAbsent(key, channel);
            if (null != oldChannel) {
                logger.info("Channel for {} already exists, so no need to store it.", key);
                channel.close();
                return oldChannel;
            } else {
                logger.debug("Storing a new channel for {}.", key);
                return channel;
            }
        } finally {
            this.closedLock.readLock().unlock();
        }
    }

    /**
     * Returns the channel bound with <code>key</code>.
     *
     * @param key Key
     * @return the channel bound with <code>key</code>.
     */
    public HChannel getChannel(T key) {
        return channels.get(key);
    }

    /**
     * Remove the channel bound with <code>key</code>.
     *
     * @param key Key
     * @return the channel bound with <code>key</code>, null if no channel
     *         is bound with <code>key</code>.
     */
    public HChannel removeChannel(T key) {
        return channels.remove(key);
    }

    /**
     * Remove the channel bound with <code>key</code>.
     *
     * @param key Key
     * @param channel The channel expected to be bound with <code>key</code>.
     * @return true if the channel is removed, false otherwise.
     */
    public boolean removeChannel(T key, HChannel channel) {
        return channels.remove(key, channel);
    }

    /**
     * Return the channels in the map.
     *
     * @return the set of channels.
     */
    public Collection<HChannel> getChannels() {
        return channels.values();
    }

    /**
     * Close the channels map.
     */
    public void close() {
        closedLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closedLock.writeLock().unlock();
        }
        logger.debug("Closing channels map.");
        for (HChannel channel : channels.values()) {
            channel.close(true);
        }
        channels.clear();
        logger.debug("Closed channels map.");
    }
}
