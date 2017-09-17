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

package org.apache.distributedlog.stream.client.impl.channel;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.stream.client.StreamSettings;
import org.apache.distributedlog.stream.proto.common.Endpoint;

/**
 * A manager manages channels to range servers.
 */
@Slf4j
public class RangeServerChannelManager implements AutoCloseable {

  private final ReentrantReadWriteLock lock;
  private boolean closed = false;
  private final ConcurrentMap<Endpoint, RangeServerChannel> channels;
  private final Function<Endpoint, RangeServerChannel> channelFactory;

  public RangeServerChannelManager(StreamSettings settings) {
    this(RangeServerChannel.factory(settings.usePlaintext()));
  }

  @VisibleForTesting
  public RangeServerChannelManager(Function<Endpoint, RangeServerChannel> channelFactory) {
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

  public boolean addRangeServer(Endpoint endpoint, RangeServerChannel channel) {
    lock.readLock().lock();
    try {
      if (closed) {
        log.warn("Skip adding channel {} of range server {} since the channel manager is already closed",
          channel, endpoint);
        channel.close();
        return false;
      }

      RangeServerChannel oldChannel = channels.putIfAbsent(endpoint, channel);
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

  public RangeServerChannel getOrCreateChannel(Endpoint endpoint) {
    RangeServerChannel channel = getChannel(endpoint);
    if (null != channel) {
      return channel;
    }
    // no channel exists
    RangeServerChannel newChannel = channelFactory.apply(endpoint);
    addRangeServer(endpoint, newChannel);
    return getChannel(endpoint);
  }

  @Nullable
  public RangeServerChannel getChannel(Endpoint endpoint) {
    lock.readLock().lock();
    try {
      return channels.get(endpoint);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Nullable
  public RangeServerChannel removeChannel(Endpoint endpoint, RangeServerChannel channel) {
    lock.readLock().lock();
    try {
      if (closed) {
        log.warn("Skip removing channel {} of range server {} since the channel manager is already closed",
          channel, endpoint);
        return null;
      }

      RangeServerChannel channelRemoved;
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
    channels.values().forEach(RangeServerChannel::close);
    channels.clear();
  }
}
