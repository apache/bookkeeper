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

package org.apache.bookkeeper.clients;

import static org.apache.bookkeeper.clients.impl.internal.ProtocolInternalUtils.createRootRangeException;
import static org.apache.bookkeeper.common.util.ListenableFutures.fromListenableFuture;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetStreamRequest;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.netty.buffer.ByteBuf;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.exceptions.ApiException;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.impl.kv.ByteBufTableImpl;
import org.apache.bookkeeper.clients.impl.kv.PByteBufSimpleTableImpl;
import org.apache.bookkeeper.clients.utils.GrpcUtils;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.ExceptionUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.stream.proto.StorageType;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc;
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc.RootRangeServiceFutureStub;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * The implementation of {@link StorageClient} client.
 */
@Slf4j
public class SimpleStorageClientImpl extends SimpleClientBase implements StorageClient {

    private static final String COMPONENT_NAME = SimpleStorageClientImpl.class.getSimpleName();

    private final String defaultNamespace;
    private final RootRangeServiceFutureStub rootRangeService;

    public SimpleStorageClientImpl(String namespaceName,
                                   StorageClientSettings settings) {
        super(settings);
        this.defaultNamespace = namespaceName;
        this.rootRangeService = GrpcUtils.configureGrpcStub(
            RootRangeServiceGrpc.newFutureStub(channel),
            Optional.empty());
    }

    public SimpleStorageClientImpl(String namespaceName,
                                   StorageClientSettings settings,
                                   Resource<OrderedScheduler> schedulerResource,
                                   ManagedChannel channel) {
        super(settings, schedulerResource, channel, false);
        this.defaultNamespace = namespaceName;
        this.rootRangeService = GrpcUtils.configureGrpcStub(
            RootRangeServiceGrpc.newFutureStub(channel),
            Optional.empty());
    }

    //
    // Materialized Views
    //

    @Override
    public CompletableFuture<PTable<ByteBuf, ByteBuf>> openPTable(String tableName) {
        return openPTable(defaultNamespace, tableName);
    }

    @Override
    public CompletableFuture<PTable<ByteBuf, ByteBuf>> openPTable(String namespaceName,
                                                                  String tableName) {
        return ExceptionUtils.callAndHandleClosedAsync(
            COMPONENT_NAME,
            isClosed(),
            (future) -> openTableImpl(namespaceName, tableName, future));
    }

    @Override
    public CompletableFuture<Table<ByteBuf, ByteBuf>> openTable(String table) {
        return openTable(defaultNamespace, table);
    }

    @Override
    public CompletableFuture<Table<ByteBuf, ByteBuf>> openTable(String namespaceName,
                                                                String table) {
        return openPTable(namespaceName, table)
            .thenApply(pTable -> new ByteBufTableImpl(pTable));
    }

    private void openTableImpl(String namespaceName,
                               String streamName,
                               CompletableFuture<PTable<ByteBuf, ByteBuf>> future) {
        CompletableFuture<StreamProperties> getStreamFuture = retryUtils.execute(() ->
            fromListenableFuture(rootRangeService.getStream(
                createGetStreamRequest(namespaceName, streamName)))
        ).thenCompose(resp -> {
            if (StatusCode.SUCCESS == resp.getCode()) {
                StreamProperties streamProps = resp.getStreamProps();
                log.info("Retrieved table properties for table {}/{} : {}",
                    namespaceName, streamName, streamProps);
                if (StorageType.TABLE != streamProps.getStreamConf().getStorageType()) {
                    return FutureUtils.exception(new ApiException(
                        "Can't open a non-table storage entity : " + streamProps.getStreamConf().getStorageType()));
                } else {
                    return FutureUtils.value(streamProps);
                }
            } else {
                return FutureUtils.exception(createRootRangeException(namespaceName, resp.getCode()));
            }
        });
        FutureUtils.proxyTo(
            getStreamFuture.thenApply(streamProps ->
                new PByteBufSimpleTableImpl(streamProps, managedChannel, CallOptions.DEFAULT, retryUtils)),
            future);
    }

}
