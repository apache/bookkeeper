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
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesRequest;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesResponse;
import org.apache.bookkeeper.stream.proto.storage.MetaRangeServiceGrpc;

/**
 * ListenableFuture-returning stub for MetaRangeService.
 *
 * <p>The lightproto gRPC plugin does not generate FutureStub classes, so this adapter
 * provides a drop-in replacement that uses {@link ClientCalls#futureUnaryCall} directly.
 */
public final class MetaRangeServiceFutureStub extends AbstractStub<MetaRangeServiceFutureStub> {

    public static MetaRangeServiceFutureStub newFutureStub(Channel channel) {
        return new MetaRangeServiceFutureStub(channel, CallOptions.DEFAULT);
    }

    private MetaRangeServiceFutureStub(Channel channel, CallOptions callOptions) {
        super(channel, callOptions);
    }

    @Override
    protected MetaRangeServiceFutureStub build(Channel channel, CallOptions callOptions) {
        return new MetaRangeServiceFutureStub(channel, callOptions);
    }

    public ListenableFuture<GetActiveRangesResponse> getActiveRanges(GetActiveRangesRequest request) {
        return ClientCalls.futureUnaryCall(
            getChannel().newCall(MetaRangeServiceGrpc.getGetActiveRangesMethod(), getCallOptions()),
            request);
    }
}
