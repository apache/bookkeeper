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

package org.apache.bookkeeper.stream.server.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import org.apache.bookkeeper.clients.utils.LongMarshaller;
import org.apache.bookkeeper.stream.server.grpc.proxy.GrpcCallProxy;
import org.apache.bookkeeper.stream.storage.api.sc.StorageContainerRegistry;

/**
 * A grpc proxy that proxies requests to storage containers.
 */
public class GrpcStorageContainerProxy<ReqT, RespT> implements ServerCallHandler<ReqT, RespT> {

    private static final String SC_ID_KEY_STR = "SC_ID";

    private static final Metadata.Key<Long> SC_ID_KEY = Metadata.Key.of(
        SC_ID_KEY_STR,
        LongMarshaller.of());

    private final StorageContainerRegistry scRegistry;

    public GrpcStorageContainerProxy(StorageContainerRegistry scRegistry) {
        this.scRegistry = scRegistry;
    }

    @Override
    public Listener<ReqT> startCall(ServerCall<ReqT, RespT> serverCall,
                                    Metadata headers) {
        Long scId = headers.get(SC_ID_KEY);

        // TODO: better handle if scId is not found in request metadata
        if (null == scId) {
            throw new UnsupportedOperationException("not implemented yet");
        }

        Channel channel = scRegistry.getStorageContainer(scId).getChannel();
        ClientCall<ReqT, RespT> clientCall =
            channel.newCall(serverCall.getMethodDescriptor(), CallOptions.DEFAULT);
        GrpcCallProxy<ReqT, RespT> callProxy =
            new GrpcCallProxy<>(serverCall, clientCall);

        clientCall.start(callProxy.getClientCallListener(), headers);
        serverCall.request(1);
        clientCall.request(1);
        return callProxy.getServerCallListener();
    }
}
