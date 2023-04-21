/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.storage.impl.service;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesRequest;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesResponse;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStreamResponse;
import org.apache.bookkeeper.stream.storage.api.metadata.RangeStoreService;

/**
 * It is a single-ton implementation that fails all requests.
 */
final class FailRequestRangeStoreService implements RangeStoreService {

    static RangeStoreService of(OrderedScheduler scheduler) {
        return new FailRequestRangeStoreService(scheduler);
    }

    private final OrderedScheduler scheduler;

    private FailRequestRangeStoreService(OrderedScheduler scheduler) {
        this.scheduler = scheduler;
    }

    private <T> CompletableFuture<T> failWrongGroupRequest() {
        CompletableFuture<T> future = FutureUtils.createFuture();
        scheduler.execute(() ->
            future.completeExceptionally(new StatusRuntimeException(Status.NOT_FOUND)));
        return future;
    }

    @Override
    public CompletableFuture<Void> start() {
        return FutureUtils.Void();
    }

    @Override
    public CompletableFuture<Void> stop() {
        return FutureUtils.Void();
    }

    //
    // Namespace API
    //

    @Override
    public CompletableFuture<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest request) {
        return failWrongGroupRequest();
    }

    @Override
    public CompletableFuture<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest request) {
        return failWrongGroupRequest();
    }

    @Override
    public CompletableFuture<GetNamespaceResponse> getNamespace(GetNamespaceRequest request) {
        return failWrongGroupRequest();
    }

    //
    // Stream API
    //

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(CreateStreamRequest request) {
        return failWrongGroupRequest();
    }

    @Override
    public CompletableFuture<DeleteStreamResponse> deleteStream(DeleteStreamRequest request) {
        return failWrongGroupRequest();
    }

    @Override
    public CompletableFuture<GetStreamResponse> getStream(GetStreamRequest request) {
        return failWrongGroupRequest();
    }

    //
    // Stream Meta Range API.
    //

    @Override
    public CompletableFuture<GetActiveRangesResponse> getActiveRanges(GetActiveRangesRequest request) {
        return failWrongGroupRequest();
    }

    //
    // Table API
    //


    @Override
    public CompletableFuture<RangeResponse> range(RangeRequest request) {
        return failWrongGroupRequest();
    }

    @Override
    public CompletableFuture<PutResponse> put(PutRequest request) {
        return failWrongGroupRequest();
    }

    @Override
    public CompletableFuture<DeleteRangeResponse> delete(DeleteRangeRequest request) {
        return failWrongGroupRequest();
    }

    @Override
    public CompletableFuture<TxnResponse> txn(TxnRequest request) {
        return failWrongGroupRequest();
    }

    @Override
    public CompletableFuture<IncrementResponse> incr(IncrementRequest request) {
        return failWrongGroupRequest();
    }
}
