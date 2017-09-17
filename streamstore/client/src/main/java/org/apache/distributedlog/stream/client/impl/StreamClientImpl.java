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

package org.apache.distributedlog.stream.client.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.common.util.ExceptionUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.distributedlog.stream.api.StreamClient;
import org.apache.distributedlog.stream.api.view.kv.Table;
import org.apache.distributedlog.stream.client.ClientResources;
import org.apache.distributedlog.stream.client.StreamSettings;
import org.apache.distributedlog.stream.client.impl.internal.RangeServerClientManagerImpl;
import org.apache.distributedlog.stream.client.impl.view.kv.TableImpl;
import org.apache.distributedlog.stream.client.internal.api.RangeServerClientManager;
import org.apache.distributedlog.stream.proto.StreamProperties;

/**
 * The implementation of {@link StreamClient} client.
 */
@Slf4j
public class StreamClientImpl extends AbstractAutoAsyncCloseable implements StreamClient {

  private static final String COMPONENT_NAME = StreamClientImpl.class.getSimpleName();

  private final String collectionName;
  private final StreamSettings settings;
  private final ClientResources resources;
  private final OrderedScheduler scheduler;

  // clients
  private final RangeServerClientManager serverManager;

  public StreamClientImpl(String collectionName,
                          StreamSettings settings,
                          ClientResources resources) {
    this.collectionName = collectionName;
    this.settings = settings;
    this.resources = resources;
    this.serverManager = new RangeServerClientManagerImpl(settings, resources.scheduler());
    this.scheduler = SharedResourceManager.shared().get(resources.scheduler());

  }

  private CompletableFuture<StreamProperties> getStreamProperties(String streamName) {
    return this.serverManager.getRootRangeClient().getStream(collectionName, streamName);
  }

  //
  // Materialized Views
  //


  @Override
  public CompletableFuture<Table> openStreamAsTable(String streamName) {
    return ExceptionUtils.callAndHandleClosedAsync(
      COMPONENT_NAME,
      isClosed(),
      (future) -> openStreamAsTableImpl(streamName, future));
  }

  private void openStreamAsTableImpl(String streamName,
                                     CompletableFuture<Table> future) {
    FutureUtils.proxyTo(
      getStreamProperties(streamName).thenComposeAsync(props -> {
        if (log.isInfoEnabled()) {
          log.info("Retrieved stream properties for stream {} : {}", streamName, props);
        }
        return new TableImpl(
          streamName,
          props,
          serverManager,
          scheduler.chooseThread(props.getStreamId())
        ).initialize();
      }),
      future
    );
  }

  //
  // Closeable API
  //

  @Override
  protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
    scheduler.submit(() -> {
      serverManager.close();
      closeFuture.complete(null);
      SharedResourceManager.shared().release(resources.scheduler(), scheduler);
      scheduler.shutdown();
    });
  }

  @Override
  public void close() {
    super.close();
    scheduler.forceShutdown(100, TimeUnit.MILLISECONDS);
  }
}
