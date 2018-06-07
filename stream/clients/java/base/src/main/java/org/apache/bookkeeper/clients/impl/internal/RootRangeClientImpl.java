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

package org.apache.bookkeeper.clients.impl.internal;

import static org.apache.bookkeeper.clients.impl.internal.ProtocolInternalUtils.createRootRangeException;
import static org.apache.bookkeeper.clients.utils.RpcUtils.isContainerNotFound;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createCreateNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createCreateStreamRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createDeleteNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createDeleteStreamRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetStreamRequest;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannelManager;
import org.apache.bookkeeper.clients.impl.internal.api.RootRangeClient;
import org.apache.bookkeeper.clients.utils.RpcUtils;
import org.apache.bookkeeper.clients.utils.RpcUtils.CreateRequestFunc;
import org.apache.bookkeeper.clients.utils.RpcUtils.ProcessRequestFunc;
import org.apache.bookkeeper.clients.utils.RpcUtils.ProcessResponseFunc;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc.RootRangeServiceFutureStub;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

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

        CompletableFuture<T> result = FutureUtils.<T>createFuture()
            .whenComplete((v, cause) -> {
                if (null != cause && isContainerNotFound(cause)) {
                    // if the rpc fails with `NOT_FOUND`, it means the storage container is not owned by any servers
                    // yet. in this case, reset the storage server channel, this allows subsequent retries will be
                    // forced to re-locate the containers.
                    scClient.resetStorageServerChannelFuture();
                }
            });
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
    // Namespace API
    //

    @Override
    public CompletableFuture<NamespaceProperties> createNamespace(String namespace,
                                                                  NamespaceConfiguration colConf) {
        return processRootRangeRpc(
            () -> createCreateNamespaceRequest(namespace, colConf),
            (rootRangeService, request) -> rootRangeService.createNamespace(request),
            (resp, resultFuture) -> processCreateNamespaceResponse(namespace, resp, resultFuture));
    }

    private void processCreateNamespaceResponse(String namespace,
                                                CreateNamespaceResponse response,
                                                CompletableFuture<NamespaceProperties> createNamespaceFuture) {
        StatusCode code = response.getCode();
        if (StatusCode.SUCCESS == code) {
            createNamespaceFuture.complete(response.getNsProps());
            return;
        }
        createNamespaceFuture.completeExceptionally(createRootRangeException(namespace, code));
    }

    @Override
    public CompletableFuture<Boolean> deleteNamespace(String namespace) {
        return processRootRangeRpc(
            () -> createDeleteNamespaceRequest(namespace),
            (rootRangeService, request) -> rootRangeService.deleteNamespace(request),
            (resp, resultFuture) -> processDeleteNamespaceResponse(namespace, resp, resultFuture));
    }

    private void processDeleteNamespaceResponse(String namespace,
                                                DeleteNamespaceResponse response,
                                                CompletableFuture<Boolean> deleteFuture) {
        StatusCode code = response.getCode();
        if (StatusCode.SUCCESS == code) {
            deleteFuture.complete(true);
            return;
        }
        deleteFuture.completeExceptionally(createRootRangeException(namespace, code));
    }

    @Override
    public CompletableFuture<NamespaceProperties> getNamespace(String namespace) {
        return processRootRangeRpc(
            () -> createGetNamespaceRequest(namespace),
            (rootRangeService, request) -> rootRangeService.getNamespace(request),
            (resp, resultFuture) -> processGetNamespaceResponse(namespace, resp, resultFuture));
    }

    private void processGetNamespaceResponse(String namespace,
                                             GetNamespaceResponse response,
                                             CompletableFuture<NamespaceProperties> getFuture) {
        StatusCode code = response.getCode();
        if (StatusCode.SUCCESS == code) {
            getFuture.complete(response.getNsProps());
            return;
        }
        getFuture.completeExceptionally(createRootRangeException(namespace, code));
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
