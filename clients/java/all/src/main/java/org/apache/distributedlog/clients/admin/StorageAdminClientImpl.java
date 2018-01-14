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

package org.apache.distributedlog.clients.admin;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.AbstractAutoAsyncCloseable;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.distributedlog.clients.config.StorageClientSettings;
import org.apache.distributedlog.clients.impl.internal.RangeServerClientManagerImpl;
import org.apache.distributedlog.clients.impl.internal.api.RangeServerClientManager;
import org.apache.distributedlog.clients.impl.internal.api.RootRangeClient;
import org.apache.distributedlog.clients.utils.ClientResources;
import org.apache.distributedlog.stream.proto.CollectionConfiguration;
import org.apache.distributedlog.stream.proto.CollectionProperties;
import org.apache.distributedlog.stream.proto.StreamConfiguration;
import org.apache.distributedlog.stream.proto.StreamProperties;

/**
 * A storage admin client.
 */
@Slf4j
public class StorageAdminClientImpl extends AbstractAutoAsyncCloseable {

  // clients
  private final RangeServerClientManager clientManager;
  private final RootRangeClient rootRangeClient;

  /**
   * Create a stream admin client with provided {@code withSettings}.
   *
   * @param settings withSettings to create an admin client.
   */
  public StorageAdminClientImpl(StorageClientSettings settings) {
    this(
      settings,
      ClientResources.create().scheduler());
  }

  /**
   * Create a stream admin client with provided {@code withSettings} and {@code scheduler}.
   *
   * @param settings withSettings to create an admin client.
   * @param schedulerResource scheduler to execute.
   */
  public StorageAdminClientImpl(StorageClientSettings settings,
                                Resource<OrderedScheduler> schedulerResource) {
    this(() -> new RangeServerClientManagerImpl(settings, schedulerResource));
  }

  @VisibleForTesting
  StorageAdminClientImpl(Supplier<RangeServerClientManager> factory) {
    this.clientManager = factory.get();
    this.rootRangeClient = this.clientManager.getRootRangeClient();
  }

  public CompletableFuture<CollectionProperties> createCollection(String collection,
                                                                  CollectionConfiguration colConf) {
    return rootRangeClient.createCollection(collection, colConf);
  }

  public CompletableFuture<Boolean> deleteCollection(String collection) {
    return rootRangeClient.deleteCollection(collection);
  }

  public CompletableFuture<CollectionProperties> getCollection(String collection) {
    return rootRangeClient.getCollection(collection);
  }

  public CompletableFuture<StreamProperties> createStream(String collection,
                                                          String streamName,
                                                          StreamConfiguration streamConf) {
    return rootRangeClient.createStream(collection, streamName, streamConf);
  }

  public CompletableFuture<Boolean> deleteStream(String collection,
                                                 String streamName) {
    return rootRangeClient.deleteStream(collection, streamName);
  }

  public CompletableFuture<StreamProperties> getStream(String collection,
                                                       String streamName) {
    return rootRangeClient.getStream(collection, streamName);
  }

  //
  // Closeable API
  //

  @Override
  protected void closeAsyncOnce(CompletableFuture<Void> closeFuture) {
    clientManager.closeAsync().whenComplete((result, cause) -> {
      closeFuture.complete(null);
    });
  }

  @Override
  public void close() {
    try {
      closeAsync().get();
    } catch (InterruptedException e) {
      log.debug("Interrupted on closing stream admin client", e);
    } catch (ExecutionException e) {
      log.debug("Failed to cloe stream admin client", e);
    }
  }
}
