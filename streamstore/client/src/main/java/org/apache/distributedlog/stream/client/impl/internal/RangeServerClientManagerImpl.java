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

package org.apache.distributedlog.stream.client.impl.internal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.distributedlog.stream.client.StreamSettings;
import org.apache.distributedlog.stream.client.impl.StorageContainerChannel;
import org.apache.distributedlog.stream.client.impl.StorageContainerChannelManager;
import org.apache.distributedlog.stream.client.impl.channel.RangeServerChannel;
import org.apache.distributedlog.stream.client.impl.channel.RangeServerChannelManager;
import org.apache.distributedlog.stream.client.internal.api.LocationClient;
import org.apache.distributedlog.stream.client.internal.api.MetaRangeClient;
import org.apache.distributedlog.stream.client.internal.api.RangeServerClientManager;
import org.apache.distributedlog.stream.client.internal.api.RootRangeClient;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.proto.common.Endpoint;

/**
 * A gRPC based {@link RangeServerClientManager} implementation.
 */
@Slf4j
public class RangeServerClientManagerImpl
    extends AbstractAutoAsyncCloseable
    implements RangeServerClientManager {

  private final Resource<OrderedScheduler> schedulerResource;
  private final OrderedScheduler scheduler;
  private final RangeServerChannelManager channelManager;
  private final StorageContainerChannelManager scChannelManager;

  // clients
  private final LocationClient locationClient;
  private final RootRangeClient rootRangeClient;
  private final StreamMetadataCache streamMetadataCache;
  private final ConcurrentMap<Long, MetaRangeClientImpl> metaRangeClients;

  public RangeServerClientManagerImpl(StreamSettings settings,
                                      Resource<OrderedScheduler> schedulerResource) {
     this(
       settings,
       schedulerResource,
       RangeServerChannel.factory(settings.usePlaintext()));
  }

  public RangeServerClientManagerImpl(StreamSettings settings,
                                      Resource<OrderedScheduler> schedulerResource,
                                      Function<Endpoint, RangeServerChannel> channelFactory) {
    this.schedulerResource = schedulerResource;
    this.scheduler = SharedResourceManager.shared().get(schedulerResource);
    this.locationClient = new LocationClientImpl(settings, scheduler);
    this.channelManager = new RangeServerChannelManager(channelFactory);
    this.scChannelManager = new StorageContainerChannelManager(
      this.channelManager,
      this.locationClient,
      scheduler);
    this.rootRangeClient = new RootRangeClientImpl(
      scheduler,
      scChannelManager);
    this.streamMetadataCache = new StreamMetadataCache(rootRangeClient);
    this.metaRangeClients = Maps.newConcurrentMap();
  }

  @VisibleForTesting
  StorageContainerChannelManager getStorageContainerChannelManager() {
    return scChannelManager;
  }

  @Override
  public StorageContainerChannel getStorageContainerChannel(long scId) {
    return scChannelManager.getOrCreate(scId);
  }

  @Override
  public LocationClient getLocationClient() {
    return this.locationClient;
  }

  @Override
  public RootRangeClient getRootRangeClient() {
    return this.rootRangeClient;
  }

  @Override
  public MetaRangeClientImpl openMetaRangeClient(StreamProperties streamProps) {
    MetaRangeClientImpl client = metaRangeClients.get(streamProps.getStreamId());
    if (null != client) {
      return client;
    }
    MetaRangeClientImpl newClient = new MetaRangeClientImpl(
      streamProps,
      scheduler,
      scChannelManager);
    MetaRangeClientImpl oldClient = metaRangeClients.putIfAbsent(
      streamProps.getStreamId(),
      newClient);
    if (null != oldClient) {
      return oldClient;
    } else {
      streamMetadataCache.putStreamProperties(streamProps.getStreamId(), streamProps);
      return newClient;
    }
  }

  @Override
  public CompletableFuture<StreamProperties> getStreamProperties(long streamId) {
    return streamMetadataCache.getStreamProperties(streamId);
  }

  @Override
  public CompletableFuture<MetaRangeClient> openMetaRangeClient(long streamId) {
    MetaRangeClientImpl client = metaRangeClients.get(streamId);
    if (null != client) {
      return FutureUtils.value(client);
    }
    return getStreamProperties(streamId).thenApply(props -> openMetaRangeClient(props));
  }

  @Override
  protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
    locationClient.close();
    channelManager.close();
    scheduler.submit(() -> {
      SharedResourceManager.shared().release(schedulerResource, scheduler);
      closeFuture.complete(null);
    });
  }
}
