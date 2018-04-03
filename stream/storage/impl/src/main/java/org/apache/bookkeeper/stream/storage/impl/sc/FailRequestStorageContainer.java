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

package org.apache.bookkeeper.stream.storage.impl.sc;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.INVALID_STORAGE_CONTAINER_ID;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.ROOT_STORAGE_CONTAINER_ID;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerResponse;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainer;

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
        scheduler.executeOrdered(scId, () -> {
            future.completeExceptionally(new StatusRuntimeException(Status.NOT_FOUND));
        });
        return future;
    }

    //
    // Namespace API
    //

    @Override
    public CompletableFuture<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest request) {
        return failWrongGroupRequest(ROOT_STORAGE_CONTAINER_ID);
    }

    @Override
    public CompletableFuture<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest request) {
        return failWrongGroupRequest(ROOT_STORAGE_CONTAINER_ID);
    }

    @Override
    public CompletableFuture<GetNamespaceResponse> getNamespace(GetNamespaceRequest request) {
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
    // Stream Meta Range API.
    //

    @Override
    public CompletableFuture<StorageContainerResponse> getActiveRanges(StorageContainerRequest request) {
        return failWrongGroupRequest(request.getScId());
    }

    //
    // Table API
    //


    @Override
    public CompletableFuture<StorageContainerResponse> range(StorageContainerRequest request) {
        return failWrongGroupRequest(request.getScId());
    }

    @Override
    public CompletableFuture<StorageContainerResponse> put(StorageContainerRequest request) {
        return failWrongGroupRequest(request.getScId());
    }

    @Override
    public CompletableFuture<StorageContainerResponse> delete(StorageContainerRequest request) {
        return failWrongGroupRequest(request.getScId());
    }

    @Override
    public CompletableFuture<StorageContainerResponse> txn(StorageContainerRequest request) {
        return failWrongGroupRequest(request.getScId());
    }

    @Override
    public CompletableFuture<StorageContainerResponse> incr(StorageContainerRequest request) {
        return failWrongGroupRequest(request.getScId());
    }
}
