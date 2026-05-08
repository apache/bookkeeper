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
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;

/**
 * ListenableFuture-returning stub for TableService.
 *
 * <p>The lightproto gRPC plugin does not generate FutureStub classes, so this adapter
 * provides a drop-in replacement that uses {@link ClientCalls#futureUnaryCall} directly.
 */
public final class TableServiceFutureStub extends AbstractStub<TableServiceFutureStub> {

    public static TableServiceFutureStub newFutureStub(Channel channel) {
        return new TableServiceFutureStub(channel, CallOptions.DEFAULT);
    }

    private TableServiceFutureStub(Channel channel, CallOptions callOptions) {
        super(channel, callOptions);
    }

    @Override
    protected TableServiceFutureStub build(Channel channel, CallOptions callOptions) {
        return new TableServiceFutureStub(channel, callOptions);
    }

    public ListenableFuture<RangeResponse> range(RangeRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(TableServiceGrpc.getRangeMethod(), getCallOptions()),
            request);
    }

    public ListenableFuture<PutResponse> put(PutRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(TableServiceGrpc.getPutMethod(), getCallOptions()),
            request);
    }

    public ListenableFuture<DeleteRangeResponse> delete(DeleteRangeRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(TableServiceGrpc.getDeleteMethod(), getCallOptions()),
            request);
    }

    public ListenableFuture<TxnResponse> txn(TxnRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(TableServiceGrpc.getTxnMethod(), getCallOptions()),
            request);
    }

    public ListenableFuture<IncrementResponse> increment(IncrementRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(TableServiceGrpc.getIncrementMethod(), getCallOptions()),
            request);
    }
}
