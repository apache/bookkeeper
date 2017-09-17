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

package org.apache.distributedlog.stream.storage.impl.sc;

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.INVALID_STORAGE_CONTAINER_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.stream.proto.rangeservice.CreateCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.CreateCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.CreateStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.CreateStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.GetCollectionRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.GetStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.GetStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerResponse;
import org.apache.distributedlog.stream.storage.api.sc.StorageContainer;

/**
 * It is a single-ton implementation that fails all requests.
 */
public final class FailRequestStorageContainer implements StorageContainer {

  public static StorageContainer of(OrderedScheduler scheduler) {
    return new FailRequestStorageContainer(scheduler);
  }

  private final OrderedScheduler scheduler;

  private FailRequestStorageContainer(OrderedScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public long getId() {
    return INVALID_STORAGE_CONTAINER_ID;
  }

  @Override
  public CompletableFuture<Void> start() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void close() {
    // no-op
  }

  private <T> CompletableFuture<T> failWrongGroupRequest(long scId) {
    CompletableFuture<T> future = FutureUtils.createFuture();
    scheduler.submitOrdered(scId, () -> {
      future.completeExceptionally(new StatusRuntimeException(Status.NOT_FOUND));
    });
    return future;
  }

  //
  // Collection API
  //

  @Override
  public CompletableFuture<CreateCollectionResponse> createCollection(CreateCollectionRequest request) {
    return failWrongGroupRequest(ROOT_STORAGE_CONTAINER_ID);
  }

  @Override
  public CompletableFuture<DeleteCollectionResponse> deleteCollection(DeleteCollectionRequest request) {
    return failWrongGroupRequest(ROOT_STORAGE_CONTAINER_ID);
  }

  @Override
  public CompletableFuture<GetCollectionResponse> getCollection(GetCollectionRequest request) {
    return failWrongGroupRequest(ROOT_STORAGE_CONTAINER_ID);
  }

  //
  // Stream API
  //

  @Override
  public CompletableFuture<CreateStreamResponse> createStream(CreateStreamRequest request) {
    return failWrongGroupRequest(ROOT_STORAGE_CONTAINER_ID);
  }

  @Override
  public CompletableFuture<DeleteStreamResponse> deleteStream(DeleteStreamRequest request) {
    return failWrongGroupRequest(ROOT_STORAGE_CONTAINER_ID);
  }

  @Override
  public CompletableFuture<GetStreamResponse> getStream(GetStreamRequest request) {
    return failWrongGroupRequest(ROOT_STORAGE_CONTAINER_ID);
  }

  //
  // Storage Container API
  //

  @Override
  public CompletableFuture<StorageContainerResponse> addStreamMetaRange(StorageContainerRequest request) {
    return failWrongGroupRequest(request.getScId());
  }

  @Override
  public CompletableFuture<StorageContainerResponse> removeStreamMetaRange(StorageContainerRequest request) {
    return failWrongGroupRequest(request.getScId());
  }

  //
  // Stream Meta Range API.
  //

  @Override
  public CompletableFuture<StorageContainerResponse> getActiveRanges(StorageContainerRequest request) {
    return failWrongGroupRequest(request.getScId());
  }

}
