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

package org.apache.distributedlog.stream.storage.impl.metadata;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.annotation.OrderedBy;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.stream.client.internal.api.RangeServerClientManager;
import org.apache.distributedlog.stream.proto.RangeMetadata;
import org.apache.distributedlog.stream.proto.rangeservice.AddStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.AddStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.GetActiveRangesResponse;
import org.apache.distributedlog.stream.proto.rangeservice.RelatedRanges;
import org.apache.distributedlog.stream.proto.rangeservice.RelationType;
import org.apache.distributedlog.stream.proto.rangeservice.RemoveStreamRequest;
import org.apache.distributedlog.stream.proto.rangeservice.RemoveStreamResponse;
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerRequest;
import org.apache.distributedlog.stream.proto.rangeservice.StorageContainerResponse;
import org.apache.distributedlog.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.distributedlog.stream.storage.api.metadata.MetaRangeStore;
import org.apache.distributedlog.stream.storage.conf.StorageConfiguration;
import org.apache.distributedlog.stream.storage.impl.metadata.stream.MetaRangeImpl;

/**
 * The default implementation of {@link MetaRangeStore}.
 */
@Slf4j
public class MetaRangeStoreImpl
    implements MetaRangeStore {

  private final RangeServerClientManager clientManager;
  private final ScheduledExecutorService executor;
  private final StorageContainerPlacementPolicy rangePlacementPolicy;

  @OrderedBy(key = "scId")
  private final Map<Long, MetaRangeImpl> streams;

  public MetaRangeStoreImpl(StorageConfiguration storageConf,
                            long scId,
                            RangeServerClientManager clientManager,
                            StorageContainerPlacementPolicy rangePlacementPolicy,
                            ScheduledExecutorService executor) {
    this.clientManager = clientManager;
    this.executor = executor;
    this.rangePlacementPolicy = rangePlacementPolicy;
    this.streams = Maps.newHashMap();
  }

  @VisibleForTesting
  Map<Long, MetaRangeImpl> unsafeGetStreams() {
    return streams;
  }

  //
  // Methods for {@link MetaRangeStore}
  //

  //
  // Stream API
  //

  @Override
  public CompletableFuture<StorageContainerResponse> addStreamMetaRange(StorageContainerRequest request) {
    return AddStreamProcessor.of().process(
      this,
      request.getAddStreamReq(),
      executor);
  }

  StatusCode verifyAddStreamRequest(AddStreamRequest request) {
    if (streams.containsKey(request.getProps().getStreamId())) {
      return StatusCode.STREAM_EXISTS;
    }
    return StatusCode.SUCCESS;
  }

  CompletableFuture<StorageContainerResponse> doProcessAddStreamRequest(AddStreamRequest request) {
    StorageContainerResponse.Builder responseBuilder = StorageContainerResponse.newBuilder();
    CompletableFuture<Boolean> response = processAddStreamCommand(request);
    CompletableFuture<StorageContainerResponse> future = FutureUtils.createFuture();
    response.whenComplete((value, cause) -> {
      if (null == cause) {
        future.complete(responseBuilder
          .setCode(StatusCode.SUCCESS)
          .setAddStreamResp(AddStreamResponse
            .newBuilder()
            .build())
          .build());
      } else {
        future.completeExceptionally(cause);
      }
    });
    return future;
  }

  CompletableFuture<Boolean>  processAddStreamCommand(AddStreamRequest request) {
    long streamId = request.getProps().getStreamId();
    MetaRangeImpl metaRange = streams.get(streamId);
    if (null != metaRange) {
      CompletableFuture<Boolean> future = FutureUtils.createFuture();
      future.completeExceptionally(
        new IllegalArgumentException("Stream " + streamId + " already exists."));
      return future;
    }
    metaRange = new MetaRangeImpl(
      executor,
      rangePlacementPolicy);
    streams.put(streamId, metaRange);

    // issue creation request
    return metaRange.create(request.getProps());
  }

  @Override
  public CompletableFuture<StorageContainerResponse> removeStreamMetaRange(StorageContainerRequest request) {
    return RemoveStreamProcessor.of().process(
      this,
      request.getRemoveStreamReq(),
      executor);
  }

  StatusCode verifyRemoveStreamRequest(RemoveStreamRequest request) {
    if (!streams.containsKey(request.getProps().getStreamId())) {
      return StatusCode.STREAM_NOT_FOUND;
    }
    return StatusCode.SUCCESS;
  }

  CompletableFuture<StorageContainerResponse> doProcessRemoveStreamRequest(RemoveStreamRequest request) {
    CompletableFuture<StorageContainerResponse> future = FutureUtils.createFuture();
    executor.submit(() -> {
      StorageContainerResponse.Builder respBuilder = StorageContainerResponse.newBuilder();
      StatusCode code = processRemoveStreamCommand(request);
      respBuilder.setCode(code);
      if (StatusCode.SUCCESS == code) {
        respBuilder
          .setRemoveStreamResp(RemoveStreamResponse.newBuilder());
      }
      future.complete(respBuilder.build());
    });
    return future;
  }

  StatusCode processRemoveStreamCommand(RemoveStreamRequest request) {
    long streamId = request.getProps().getStreamId();
    MetaRangeImpl metaRange = streams.remove(streamId);
    if (null == metaRange) {
      return StatusCode.STREAM_NOT_FOUND;
    } else {
      return StatusCode.SUCCESS;
    }
  }

  @Override
  public CompletableFuture<StorageContainerResponse> getActiveRanges(StorageContainerRequest request) {
    checkState(
      StorageContainerRequest.Type.GET_ACTIVE_RANGES == request.getType(),
      "Wrong request type: %s", request.getType());
    CompletableFuture<StorageContainerResponse> future = FutureUtils.createFuture();
    long streamId = request.getGetActiveRangesReq().getStreamId();
    StorageContainerResponse.Builder responseBuilder = StorageContainerResponse.newBuilder();
    GetActiveRangesResponse.Builder builder = GetActiveRangesResponse.newBuilder();
    MetaRangeImpl metaRange = streams.get(streamId);

    if (null == metaRange) {
      responseBuilder
        .setCode(StatusCode.STREAM_NOT_FOUND)
        .setGetActiveRangesResp(GetActiveRangesResponse.getDefaultInstance());
      return FutureUtils.value(responseBuilder.build());
    }

    metaRange.getActiveRanges().whenComplete((ranges, cause) -> {
      if (null == cause) {
        for (RangeMetadata range : ranges) {
          RelatedRanges.Builder rrBuilder = RelatedRanges.newBuilder()
            .setProps(range.getProps())
            .setType(RelationType.PARENTS)
            .addAllRelatedRanges(range.getParentsList());
          builder.addRanges(rrBuilder);
        }

        future.complete(responseBuilder
          .setCode(StatusCode.SUCCESS)
          .setGetActiveRangesResp(builder.build())
          .build());
      } else {
        future.completeExceptionally(cause);
      }
    });

    return future;
  }

}
