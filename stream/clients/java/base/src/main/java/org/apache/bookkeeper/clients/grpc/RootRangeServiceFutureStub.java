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

package org.apache.bookkeeper.clients.grpc;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
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
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc;

/**
 * ListenableFuture-returning stub for RootRangeService.
 *
 * <p>The lightproto gRPC plugin does not generate FutureStub classes, so this adapter
 * provides a drop-in replacement that uses {@link ClientCalls#futureUnaryCall} directly.
 */
public final class RootRangeServiceFutureStub extends AbstractStub<RootRangeServiceFutureStub> {

    public static RootRangeServiceFutureStub newFutureStub(Channel channel) {
        return new RootRangeServiceFutureStub(channel, CallOptions.DEFAULT);
    }

    private RootRangeServiceFutureStub(Channel channel, CallOptions callOptions) {
        super(channel, callOptions);
    }

    @Override
    protected RootRangeServiceFutureStub build(Channel channel, CallOptions callOptions) {
        return new RootRangeServiceFutureStub(channel, callOptions);
    }

    public ListenableFuture<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(RootRangeServiceGrpc.getCreateNamespaceMethod(), getCallOptions()),
            request);
    }

    public ListenableFuture<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(RootRangeServiceGrpc.getDeleteNamespaceMethod(), getCallOptions()),
            request);
    }

    public ListenableFuture<GetNamespaceResponse> getNamespace(GetNamespaceRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(RootRangeServiceGrpc.getGetNamespaceMethod(), getCallOptions()),
            request);
    }

    public ListenableFuture<CreateStreamResponse> createStream(CreateStreamRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(RootRangeServiceGrpc.getCreateStreamMethod(), getCallOptions()),
            request);
    }

    public ListenableFuture<DeleteStreamResponse> deleteStream(DeleteStreamRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(RootRangeServiceGrpc.getDeleteStreamMethod(), getCallOptions()),
            request);
    }

    public ListenableFuture<GetStreamResponse> getStream(GetStreamRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(RootRangeServiceGrpc.getGetStreamMethod(), getCallOptions()),
            request);
    }
}
