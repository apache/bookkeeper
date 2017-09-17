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

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.INVALID_COLLECTION_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.MIN_DATA_STREAM_ID;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.validateCollectionName;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.validateStreamName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.annotation.OrderedBy;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.stream.client.internal.api.RangeServerClientManager;
import org.apache.distributedlog.stream.proto.CollectionMetadata;
import org.apache.distributedlog.stream.proto.CollectionProperties;
import org.apache.distributedlog.stream.proto.StreamName;
import org.apache.distributedlog.stream.proto.StreamProperties;
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
import org.apache.distributedlog.stream.proto.rangeservice.StatusCode;
import org.apache.distributedlog.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.distributedlog.stream.storage.api.metadata.RootRangeStore;

/**
 * A default in-memory implementation of {@link RootRangeStore}.
 */
@Slf4j
public class RootRangeStoreImpl
    implements RootRangeStore {

  private final RangeServerClientManager clientManager;
  private final StorageContainerPlacementPolicy placementPolicy;
  private final ScheduledExecutorService executor;

  // Collections
  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private long nextCollectionId = INVALID_COLLECTION_ID;
  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private final Map<String, CollectionImpl> collectionNames = Maps.newHashMap();
  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private final Map<Long, CollectionImpl> collections = Maps.newHashMap();
  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private final Map<Long, CollectionImpl> streamToCollections = Maps.newHashMap();

  // Streams
  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private long nextStreamId = MIN_DATA_STREAM_ID;

  public RootRangeStoreImpl(RangeServerClientManager clientManager,
                            StorageContainerPlacementPolicy placementPolicy,
                            ScheduledExecutorService executor) {
    this.clientManager = clientManager;
    this.placementPolicy = placementPolicy;
    this.executor = executor;
  }

  //
  // Methods for {@link RootRangeStore}
  //

  @VisibleForTesting
  long unsafeGetNextCollectionId() {
    return nextCollectionId;
  }

  @VisibleForTesting
  long unsafeGetNextStreamId() {
    return nextStreamId;
  }

  @VisibleForTesting
  Map<String, CollectionImpl> unsafeGetCollectionNames() {
    return collectionNames;
  }

  @VisibleForTesting
  Map<Long, CollectionImpl> unsafeGetCollections() {
    return collections;
  }

  //
  // Collections API
  //

  @Override
  public CompletableFuture<CreateCollectionResponse> createCollection(CreateCollectionRequest request) {
    return CreateCollectionProcessor.of().process(
      this,
      request,
      executor);
  }

  StatusCode verifyCreateCollectionRequest(CreateCollectionRequest request) {
    String colName = request.getName();
    StatusCode code = StatusCode.SUCCESS;
    if (!validateCollectionName(colName)) {
      log.error("Failed to create collection due to invalid collection name {}", colName);
      code = StatusCode.INVALID_COLLECTION_NAME;
    } else if (collectionNames.containsKey(colName)) {
      log.error("Failed to create collection due to collection {} already exists", colName);
      code = StatusCode.COLLECTION_EXISTS;
    }
    return code;
  }

  CompletableFuture<CreateCollectionResponse> doProcessCreateCollectionRequest(CreateCollectionRequest request) {
    CompletableFuture<CreateCollectionResponse> future = FutureUtils.createFuture();
    executor.submit(() -> {
      long collectionId = nextCollectionId++;
      String colName = request.getName();
      CollectionMetadata metadata = CollectionMetadata.newBuilder()
        .setProps(CollectionProperties.newBuilder()
          .setCollectionId(collectionId)
          .setCollectionName(colName)
          .setDefaultStreamConf(request.getColConf().getDefaultStreamConf()))
        .build();
      CreateCollectionResponse.Builder respBuilder = CreateCollectionResponse.newBuilder();
      if (processCreateCollectionCommand(metadata)) {
        respBuilder.setCode(StatusCode.SUCCESS);
        respBuilder.setColProps(metadata.getProps());
      } else {
        respBuilder.setCode(StatusCode.COLLECTION_EXISTS);
      }
      future.complete(respBuilder.build());
    });
    return future;
  }

  boolean processCreateCollectionCommand(CollectionMetadata metadata) {
    long collectionId = metadata.getProps().getCollectionId();
    String colName  = metadata.getProps().getCollectionName();
    CollectionImpl collection = CollectionImpl.of(collectionId, colName);
    collection.setMetadata(metadata);
    if (null == collections.putIfAbsent(collectionId, collection)) {
      collectionNames.putIfAbsent(colName, collection);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public CompletableFuture<DeleteCollectionResponse> deleteCollection(DeleteCollectionRequest request) {
    return DeleteCollectionProcessor.of().process(
      this,
      request,
      executor);
  }

  StatusCode verifyDeleteCollectionRequest(DeleteCollectionRequest request) {
    String colName = request.getName();
    StatusCode code = StatusCode.SUCCESS;
    if (!validateCollectionName(colName)) {
      log.error("Failed to delete collection due to invalid collection name {}", colName);
      code = StatusCode.INVALID_COLLECTION_NAME;
    } else if (!collectionNames.containsKey(colName)) {
      log.error("Failed to delete collection due to collection {} is not found", colName);
      code = StatusCode.COLLECTION_NOT_FOUND;
    }
    return code;
  }

  CompletableFuture<DeleteCollectionResponse> doProcessDeleteCollectionRequest(DeleteCollectionRequest request) {
    CompletableFuture<DeleteCollectionResponse> future = FutureUtils.createFuture();
    executor.submit(() -> {
      DeleteCollectionResponse.Builder respBuilder = DeleteCollectionResponse.newBuilder();
      StatusCode code = processDeleteCollectionCommand(request);
      respBuilder.setCode(code);
      future.complete(respBuilder.build());
    });
    return future;
  }

  StatusCode processDeleteCollectionCommand(DeleteCollectionRequest request) {
    CollectionImpl collection = collectionNames.get(request.getName());
    if (null == collection) {
      return StatusCode.COLLECTION_NOT_FOUND;
    } else {
      String colName = collection.getName();
      collectionNames.remove(colName, collection);
      return StatusCode.SUCCESS;
    }
  }

  @Override
  public CompletableFuture<GetCollectionResponse> getCollection(GetCollectionRequest request) {
    CompletableFuture<GetCollectionResponse> getFuture = FutureUtils.createFuture();
    executor.submit(() -> unsafeGetCollection(getFuture, request.getName()));
    return getFuture;
  }

  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private void unsafeGetCollection(CompletableFuture<GetCollectionResponse> getFuture,
                                   String colName) {
    GetCollectionResponse.Builder respBuilder = GetCollectionResponse.newBuilder();
    StatusCode code;
    if (!validateCollectionName(colName)) {
      log.error("Failed to get collection due to invalid collection name {}", colName);
      code = StatusCode.INVALID_COLLECTION_NAME;
    } else {
      CollectionImpl collection = collectionNames.get(colName);
      if (null == collection) {
        code = StatusCode.COLLECTION_NOT_FOUND;
      } else {
        code = StatusCode.SUCCESS;
        respBuilder.setColProps(collection.getMetadata().getProps());
      }
    }
    respBuilder.setCode(code);
    getFuture.complete(respBuilder.build());
  }

  //
  // Stream API
  //

  @Override
  public CompletableFuture<CreateStreamResponse> createStream(CreateStreamRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<DeleteStreamResponse> deleteStream(DeleteStreamRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<GetStreamResponse> getStream(GetStreamRequest request) {
    CompletableFuture<GetStreamResponse> getFuture = FutureUtils.createFuture();
    switch (request.getIdCase()) {
      case STREAM_ID:
        long streamId = request.getStreamId();
        executor.submit(() -> unsafeGetStream(getFuture, streamId));
        break;
      case STREAM_NAME:
        StreamName streamName = request.getStreamName();
        executor.submit(() -> unsafeGetStream(
          getFuture, streamName.getColName(), streamName.getStreamName()));
        break;
      default:
        getFuture.complete(GetStreamResponse.newBuilder()
          .setCode(StatusCode.BAD_REQUEST)
          .build());
        break;
    }
    return getFuture;
  }

  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private void unsafeGetStream(CompletableFuture<GetStreamResponse> getFuture,
                               long streamId) {
    GetStreamResponse.Builder respBuilder = GetStreamResponse.newBuilder();
    StatusCode code;
    CollectionImpl collection = streamToCollections.get(streamId);
    if (null == collection) {
      code = StatusCode.STREAM_NOT_FOUND;
    } else {
      StreamProperties streamProps = collection.getStream(streamId);
      if (null == streamProps) {
        code = StatusCode.STREAM_NOT_FOUND;
      } else {
        code = StatusCode.SUCCESS;
        respBuilder.setStreamProps(streamProps);
      }
    }
    respBuilder.setCode(code);
    getFuture.complete(respBuilder.build());
  }

  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private void unsafeGetStream(CompletableFuture<GetStreamResponse> getFuture,
                               String colName,
                               String streamName) {
    GetStreamResponse.Builder respBuilder = GetStreamResponse.newBuilder();
    StatusCode code;
    if (!validateStreamName(streamName)) {
      log.error("Failed to get stream due to invalid stream name {}", streamName);
      code = StatusCode.INVALID_STREAM_NAME;
    } else {
      CollectionImpl collection = collectionNames.get(colName);
      if (null == collection) {
        code = StatusCode.COLLECTION_NOT_FOUND;
      } else {
        StreamProperties streamProps = collection.getStream(streamName);
        if (null == streamProps) {
          code = StatusCode.STREAM_NOT_FOUND;
        } else {
          code = StatusCode.SUCCESS;
          respBuilder.setStreamProps(streamProps);
        }
      }
    }
    respBuilder.setCode(code);
    getFuture.complete(respBuilder.build());
  }

}
