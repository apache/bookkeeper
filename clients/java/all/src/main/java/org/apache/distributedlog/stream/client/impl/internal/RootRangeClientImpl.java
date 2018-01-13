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

import static org.apache.distributedlog.stream.client.impl.internal.ProtocolInternalUtils.createRootRangeException;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createCreateCollectionRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createCreateStreamRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createDeleteCollectionRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createDeleteStreamRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createGetCollectionRequest;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.createGetStreamRequest;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.stream.client.exceptions.ClientException;
import org.apache.distributedlog.stream.client.impl.StorageContainerChannel;
import org.apache.distributedlog.stream.client.impl.StorageContainerChannelManager;
import org.apache.distributedlog.stream.client.internal.api.RootRangeClient;
import org.apache.distributedlog.stream.client.utils.RpcUtils;
import org.apache.distributedlog.stream.client.utils.RpcUtils.CreateRequestFunc;
import org.apache.distributedlog.stream.client.utils.RpcUtils.ProcessRequestFunc;
import org.apache.distributedlog.stream.client.utils.RpcUtils.ProcessResponseFunc;
import org.apache.distributedlog.stream.proto.CollectionConfiguration;
import org.apache.distributedlog.stream.proto.CollectionProperties;
import org.apache.distributedlog.stream.proto.StreamConfiguration;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.proto.rangeservice.CreateCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.CreateStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.DeleteStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.GetCollectionResponse;
import org.apache.distributedlog.stream.proto.rangeservice.GetStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.RootRangeServiceGrpc.RootRangeServiceFutureStub;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;

/**
 * A default implementation for {@link RootRangeClient}.
 */
@Slf4j
class RootRangeClientImpl implements RootRangeClient {

  private final ScheduledExecutorService executor;
  private final StorageContainerChannel scClient;

  RootRangeClientImpl(OrderedScheduler scheduler,
                      StorageContainerChannelManager channelManager) {
    this.executor = scheduler.chooseThread(ROOT_STORAGE_CONTAINER_ID);
    this.scClient = channelManager.getOrCreate(ROOT_STORAGE_CONTAINER_ID);
  }

  @VisibleForTesting
  StorageContainerChannel getStorageContainerClient() {
    return scClient;
  }

  private <T, ReqT, RespT> CompletableFuture<T> processRootRangeRpc(
      CreateRequestFunc<ReqT> createRequestFunc,
      ProcessRequestFunc<ReqT, RespT, RootRangeServiceFutureStub> processRequestFunc,
      ProcessResponseFunc<RespT, T> processResponseFunc) {

    CompletableFuture<T> result = FutureUtils.createFuture();
    scClient.getStorageContainerChannelFuture().whenComplete((rsChannel, cause) -> {
      if (null != cause) {
        handleGetRootRangeServiceFailure(result, cause);
        return;
      }
      RpcUtils.processRpc(
        rsChannel.getRootRangeService(),
        result,
        createRequestFunc,
        processRequestFunc,
        processResponseFunc
      );
    });
    return result;
  }

  //
  // Collection API
  //

  @Override
  public CompletableFuture<CollectionProperties> createCollection(String collection,
                                                                  CollectionConfiguration colConf) {
    return processRootRangeRpc(
      () -> createCreateCollectionRequest(collection, colConf),
      (rootRangeService, request) -> rootRangeService.createCollection(request),
      (resp, resultFuture) -> processCreateCollectionResponse(collection, resp, resultFuture));
  }

  private void processCreateCollectionResponse(String collection,
                                               CreateCollectionResponse response,
                                               CompletableFuture<CollectionProperties> createCollectionFuture) {
    StatusCode code = response.getCode();
    if (StatusCode.SUCCESS == code) {
      createCollectionFuture.complete(response.getColProps());
      return;
    }
    createCollectionFuture.completeExceptionally(createRootRangeException(collection, code));
  }

  @Override
  public CompletableFuture<Boolean> deleteCollection(String collection) {
    return processRootRangeRpc(
      () -> createDeleteCollectionRequest(collection),
      (rootRangeService, request) -> rootRangeService.deleteCollection(request),
      (resp, resultFuture) -> processDeleteCollectionResponse(collection, resp, resultFuture));
  }

  private void processDeleteCollectionResponse(String collection,
                                               DeleteCollectionResponse response,
                                               CompletableFuture<Boolean> deleteFuture) {
    StatusCode code = response.getCode();
    if (StatusCode.SUCCESS == code) {
      deleteFuture.complete(true);
      return;
    }
    deleteFuture.completeExceptionally(createRootRangeException(collection, code));
  }

  @Override
  public CompletableFuture<CollectionProperties> getCollection(String collection) {
    return processRootRangeRpc(
      () -> createGetCollectionRequest(collection),
      (rootRangeService, request) -> rootRangeService.getCollection(request),
      (resp, resultFuture) -> processGetCollectionResponse(collection, resp, resultFuture));
  }

  private void processGetCollectionResponse(String collection,
                                            GetCollectionResponse response,
                                            CompletableFuture<CollectionProperties> getFuture) {
    StatusCode code = response.getCode();
    if (StatusCode.SUCCESS == code) {
      getFuture.complete(response.getColProps());
      return;
    }
    getFuture.completeExceptionally(createRootRangeException(collection, code));
  }


  //
  // Stream API
  //

  @Override
  public CompletableFuture<StreamProperties> createStream(String colName,
                                                          String streamName,
                                                          StreamConfiguration streamConf) {
    return processRootRangeRpc(
      () -> createCreateStreamRequest(colName, streamName, streamConf),
      (rootRangeService, request) -> rootRangeService.createStream(request),
      (resp, resultFuture) -> processCreateStreamResponse(streamName, resp, resultFuture));
  }

  private void processCreateStreamResponse(String streamName,
                                           CreateStreamResponse response,
                                           CompletableFuture<StreamProperties> createStreamFuture) {
    StatusCode code = response.getCode();
    if (StatusCode.SUCCESS == code) {
      createStreamFuture.complete(response.getStreamProps());
      return;
    }
    createStreamFuture.completeExceptionally(createRootRangeException(streamName, code));
  }

  @Override
  public CompletableFuture<StreamProperties> getStream(String colName, String streamName) {
    return processRootRangeRpc(
      () -> createGetStreamRequest(colName, streamName),
      (rootRangeService, request) -> rootRangeService.getStream(request),
      (resp, resultFuture) -> processGetStreamResponse(streamName, resp, resultFuture));
  }

  @Override
  public CompletableFuture<StreamProperties> getStream(long streamId) {
    return processRootRangeRpc(
      () -> createGetStreamRequest(streamId),
      (rootRangeService, request) -> rootRangeService.getStream(request),
      (resp, resultFuture) -> processGetStreamResponse("Stream(" + streamId + ")", resp, resultFuture));
  }

  private void processGetStreamResponse(String streamName,
                                        GetStreamResponse response,
                                        CompletableFuture<StreamProperties> getStreamFuture) {
    StatusCode code = response.getCode();
    if (StatusCode.SUCCESS == code) {
      getStreamFuture.complete(response.getStreamProps());
      return;
    }
    getStreamFuture.completeExceptionally(createRootRangeException(streamName, code));
  }

  @Override
  public CompletableFuture<Boolean> deleteStream(String colName, String streamName) {
    return processRootRangeRpc(
      () -> createDeleteStreamRequest(colName, streamName),
      (rootRangeService, request) -> rootRangeService.deleteStream(request),
      (resp, resultFuture) -> processDeleteStreamResponse(streamName, resp, resultFuture));
  }

  private void processDeleteStreamResponse(String streamName,
                                           DeleteStreamResponse response,
                                           CompletableFuture<Boolean> deleteStreamFuture) {
    StatusCode code = response.getCode();
    if (StatusCode.SUCCESS == code) {
      deleteStreamFuture.complete(true);
      return;
    }
    deleteStreamFuture.completeExceptionally(createRootRangeException(streamName, code));
  }

  private void handleGetRootRangeServiceFailure(CompletableFuture<?> future, Throwable cause) {
    future.completeExceptionally(new ClientException("GetRootRangeService is unexpected to fail", cause));
  }

}
