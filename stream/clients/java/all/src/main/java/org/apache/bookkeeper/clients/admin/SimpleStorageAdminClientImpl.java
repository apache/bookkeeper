/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.clients.admin;

import static org.apache.bookkeeper.clients.impl.internal.ProtocolInternalUtils.createRootRangeException;
import static org.apache.bookkeeper.common.util.ListenableFutures.fromListenableFuture;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createCreateNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createCreateStreamRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createDeleteNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createDeleteStreamRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetStreamRequest;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.clients.SimpleClientBase;
import org.apache.bookkeeper.clients.SimpleStorageClientImpl;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.utils.ClientResources;
import org.apache.bookkeeper.clients.utils.GrpcUtils;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc;
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc.RootRangeServiceFutureStub;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * A simple implementation of {@link StorageAdminClient}.
 *
 * <p>Unlike {@link StorageAdminClientImpl} which handles all the routing logic, this implementation
 * just make simple grpc calls to the storage service, the storage service handles the grpc requests
 * and handle proper routing at the server side.
 */
public class SimpleStorageAdminClientImpl extends SimpleClientBase implements StorageAdminClient {

    private final RootRangeServiceFutureStub rootRangeService;

    public SimpleStorageAdminClientImpl(StorageClientSettings settings) {
        this(settings, ClientResources.create().scheduler());
    }

    public SimpleStorageAdminClientImpl(StorageClientSettings settings,
                                        Resource<OrderedScheduler> schedulerResource) {
        super(settings, schedulerResource);
        this.rootRangeService = GrpcUtils.configureGrpcStub(
            RootRangeServiceGrpc.newFutureStub(channel),
            Optional.empty());
    }

    @Override
    public StorageClient asClient(String namespace) {
        return new SimpleStorageClientImpl(
            namespace,
            settings,
            schedulerResource,
            managedChannel
        );
    }

    @Override
    public CompletableFuture<NamespaceProperties> createNamespace(String namespace, NamespaceConfiguration conf) {
        return retryUtils.execute(() ->
            fromListenableFuture(rootRangeService.createNamespace(createCreateNamespaceRequest(namespace, conf)))
        ).thenCompose(resp -> {
            if (StatusCode.SUCCESS == resp.getCode()) {
                return FutureUtils.value(resp.getNsProps());
            } else {
                return FutureUtils.exception(createRootRangeException(namespace, resp.getCode()));
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> deleteNamespace(String namespace) {
        return retryUtils.execute(() ->
            fromListenableFuture(rootRangeService.deleteNamespace(createDeleteNamespaceRequest(namespace)))
        ).thenCompose(resp -> {
            if (StatusCode.SUCCESS == resp.getCode()) {
                return FutureUtils.value(true);
            } else {
                return FutureUtils.exception(createRootRangeException(namespace, resp.getCode()));
            }
        });
    }

    @Override
    public CompletableFuture<NamespaceProperties> getNamespace(String namespace) {
        return retryUtils.execute(() ->
            fromListenableFuture(rootRangeService.getNamespace(createGetNamespaceRequest(namespace)))
        ).thenCompose(resp -> {
            if (StatusCode.SUCCESS == resp.getCode()) {
                return FutureUtils.value(resp.getNsProps());
            } else {
                return FutureUtils.exception(createRootRangeException(namespace, resp.getCode()));
            }
        });
    }

    @Override
    public CompletableFuture<StreamProperties> createStream(String namespace,
                                                            String streamName,
                                                            StreamConfiguration streamConfiguration) {
        return retryUtils.execute(() ->
            fromListenableFuture(rootRangeService.createStream(
                createCreateStreamRequest(namespace, streamName, streamConfiguration)))
        ).thenCompose(resp -> {
            if (StatusCode.SUCCESS == resp.getCode()) {
                return FutureUtils.value(resp.getStreamProps());
            } else {
                return FutureUtils.exception(createRootRangeException(namespace, resp.getCode()));
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> deleteStream(String namespace, String streamName) {
        return retryUtils.execute(() ->
            fromListenableFuture(rootRangeService.deleteStream(
                createDeleteStreamRequest(namespace, streamName)))
        ).thenCompose(resp -> {
            if (StatusCode.SUCCESS == resp.getCode()) {
                return FutureUtils.value(true);
            } else {
                return FutureUtils.exception(createRootRangeException(namespace, resp.getCode()));
            }
        });
    }

    @Override
    public CompletableFuture<StreamProperties> getStream(String namespace, String streamName) {
        return retryUtils.execute(() ->
            fromListenableFuture(rootRangeService.getStream(
                createGetStreamRequest(namespace, streamName)))
        ).thenCompose(resp -> {
            if (StatusCode.SUCCESS == resp.getCode()) {
                return FutureUtils.value(resp.getStreamProps());
            } else {
                return FutureUtils.exception(createRootRangeException(namespace, resp.getCode()));
            }
        });
    }

}
