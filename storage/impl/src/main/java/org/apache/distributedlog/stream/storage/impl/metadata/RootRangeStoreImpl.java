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

import static org.apache.distributedlog.stream.protocol.ProtocolConstants.INVALID_NAMESPACE_ID;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.MIN_DATA_STREAM_ID;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.validateNamespaceName;
import static org.apache.distributedlog.stream.protocol.util.ProtoUtils.validateStreamName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.annotation.OrderedBy;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.distributedlog.stream.proto.NamespaceMetadata;
import org.apache.distributedlog.stream.proto.NamespaceProperties;
import org.apache.distributedlog.stream.proto.StreamName;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.proto.storage.CreateNamespaceRequest;
import org.apache.distributedlog.stream.proto.storage.CreateNamespaceResponse;
import org.apache.distributedlog.stream.proto.storage.CreateStreamRequest;
import org.apache.distributedlog.stream.proto.storage.CreateStreamResponse;
import org.apache.distributedlog.stream.proto.storage.DeleteNamespaceRequest;
import org.apache.distributedlog.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.distributedlog.stream.proto.storage.DeleteStreamRequest;
import org.apache.distributedlog.stream.proto.storage.DeleteStreamResponse;
import org.apache.distributedlog.stream.proto.storage.GetNamespaceRequest;
import org.apache.distributedlog.stream.proto.storage.GetNamespaceResponse;
import org.apache.distributedlog.stream.proto.storage.GetStreamRequest;
import org.apache.distributedlog.stream.proto.storage.GetStreamResponse;
import org.apache.distributedlog.stream.proto.storage.StatusCode;
import org.apache.distributedlog.stream.protocol.util.StorageContainerPlacementPolicy;
import org.apache.distributedlog.stream.storage.api.metadata.RootRangeStore;

/**
 * A default in-memory implementation of {@link RootRangeStore}.
 */
@Slf4j
public class RootRangeStoreImpl
    implements RootRangeStore {

  private final MVCCAsyncStore<byte[], byte[]> store;
  private final StorageContainerPlacementPolicy placementPolicy;
  private final ScheduledExecutorService executor;

  // Namespaces
  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private long nextNamespaceId = INVALID_NAMESPACE_ID;
  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private final Map<String, NamespaceImpl> namespaceNames = Maps.newHashMap();
  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private final Map<Long, NamespaceImpl> namespaces = Maps.newHashMap();
  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private final Map<Long, NamespaceImpl> streamToNamespaces = Maps.newHashMap();

  // Streams
  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private long nextStreamId = MIN_DATA_STREAM_ID;

  public RootRangeStoreImpl(MVCCAsyncStore<byte[], byte[]> store,
                            StorageContainerPlacementPolicy placementPolicy,
                            ScheduledExecutorService executor) {
    this.store = store;
    this.placementPolicy = placementPolicy;
    this.executor = executor;
  }

  //
  // Methods for {@link RootRangeStore}
  //

  @VisibleForTesting
  long unsafeGetNextNamespaceId() {
    return nextNamespaceId;
  }

  @VisibleForTesting
  long unsafeGetNextStreamId() {
    return nextStreamId;
  }

  @VisibleForTesting
  Map<String, NamespaceImpl> unsafeGetNamespaceNames() {
    return namespaceNames;
  }

  @VisibleForTesting
  Map<Long, NamespaceImpl> unsafeGetNamespaces() {
    return namespaces;
  }

  //
  // Namespaces API
  //

  @Override
  public CompletableFuture<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest request) {
    return CreateNamespaceProcessor.of().process(
      this,
      request,
      executor);
  }

  StatusCode verifyCreateNamespaceRequest(CreateNamespaceRequest request) {
    String colName = request.getName();
    StatusCode code = StatusCode.SUCCESS;
    if (!validateNamespaceName(colName)) {
      log.error("Failed to create namespace due to invalid namespace name {}", colName);
      code = StatusCode.INVALID_NAMESPACE_NAME;
    } else if (namespaceNames.containsKey(colName)) {
      log.error("Failed to create namespace due to namespace {} already exists", colName);
      code = StatusCode.NAMESPACE_EXISTS;
    }
    return code;
  }

  CompletableFuture<CreateNamespaceResponse> doProcessCreateNamespaceRequest(CreateNamespaceRequest request) {
    CompletableFuture<CreateNamespaceResponse> future = FutureUtils.createFuture();
    executor.submit(() -> {
      long namespaceId = nextNamespaceId++;
      String colName = request.getName();
      NamespaceMetadata metadata = NamespaceMetadata.newBuilder()
        .setProps(NamespaceProperties.newBuilder()
          .setNamespaceId(namespaceId)
          .setNamespaceName(colName)
          .setDefaultStreamConf(request.getColConf().getDefaultStreamConf()))
        .build();
      CreateNamespaceResponse.Builder respBuilder = CreateNamespaceResponse.newBuilder();
      if (processCreateNamespaceCommand(metadata)) {
        respBuilder.setCode(StatusCode.SUCCESS);
        respBuilder.setColProps(metadata.getProps());
      } else {
        respBuilder.setCode(StatusCode.NAMESPACE_EXISTS);
      }
      future.complete(respBuilder.build());
    });
    return future;
  }

  boolean processCreateNamespaceCommand(NamespaceMetadata metadata) {
    long namespaceId = metadata.getProps().getNamespaceId();
    String colName  = metadata.getProps().getNamespaceName();
    NamespaceImpl namespace = NamespaceImpl.of(namespaceId, colName);
    namespace.setMetadata(metadata);
    if (null == namespaces.putIfAbsent(namespaceId, namespace)) {
      namespaceNames.putIfAbsent(colName, namespace);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public CompletableFuture<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest request) {
    return DeleteNamespaceProcessor.of().process(
      this,
      request,
      executor);
  }

  StatusCode verifyDeleteNamespaceRequest(DeleteNamespaceRequest request) {
    String colName = request.getName();
    StatusCode code = StatusCode.SUCCESS;
    if (!validateNamespaceName(colName)) {
      log.error("Failed to delete namespace due to invalid namespace name {}", colName);
      code = StatusCode.INVALID_NAMESPACE_NAME;
    } else if (!namespaceNames.containsKey(colName)) {
      log.error("Failed to delete namespace due to namespace {} is not found", colName);
      code = StatusCode.NAMESPACE_NOT_FOUND;
    }
    return code;
  }

  CompletableFuture<DeleteNamespaceResponse> doProcessDeleteNamespaceRequest(DeleteNamespaceRequest request) {
    CompletableFuture<DeleteNamespaceResponse> future = FutureUtils.createFuture();
    executor.submit(() -> {
      DeleteNamespaceResponse.Builder respBuilder = DeleteNamespaceResponse.newBuilder();
      StatusCode code = processDeleteNamespaceCommand(request);
      respBuilder.setCode(code);
      future.complete(respBuilder.build());
    });
    return future;
  }

  StatusCode processDeleteNamespaceCommand(DeleteNamespaceRequest request) {
    NamespaceImpl namespace = namespaceNames.get(request.getName());
    if (null == namespace) {
      return StatusCode.NAMESPACE_NOT_FOUND;
    } else {
      String colName = namespace.getName();
      namespaceNames.remove(colName, namespace);
      return StatusCode.SUCCESS;
    }
  }

  @Override
  public CompletableFuture<GetNamespaceResponse> getNamespace(GetNamespaceRequest request) {
    CompletableFuture<GetNamespaceResponse> getFuture = FutureUtils.createFuture();
    executor.submit(() -> unsafeGetNamespace(getFuture, request.getName()));
    return getFuture;
  }

  @OrderedBy(key = "ROOT_STORAGE_CONTAINER_ID")
  private void unsafeGetNamespace(CompletableFuture<GetNamespaceResponse> getFuture,
                                   String colName) {
    GetNamespaceResponse.Builder respBuilder = GetNamespaceResponse.newBuilder();
    StatusCode code;
    if (!validateNamespaceName(colName)) {
      log.error("Failed to get namespace due to invalid namespace name {}", colName);
      code = StatusCode.INVALID_NAMESPACE_NAME;
    } else {
      NamespaceImpl namespace = namespaceNames.get(colName);
      if (null == namespace) {
        code = StatusCode.NAMESPACE_NOT_FOUND;
      } else {
        code = StatusCode.SUCCESS;
        respBuilder.setColProps(namespace.getMetadata().getProps());
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
    NamespaceImpl namespace = streamToNamespaces.get(streamId);
    if (null == namespace) {
      code = StatusCode.STREAM_NOT_FOUND;
    } else {
      StreamProperties streamProps = namespace.getStream(streamId);
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
      NamespaceImpl namespace = namespaceNames.get(colName);
      if (null == namespace) {
        code = StatusCode.NAMESPACE_NOT_FOUND;
      } else {
        StreamProperties streamProps = namespace.getStream(streamName);
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
