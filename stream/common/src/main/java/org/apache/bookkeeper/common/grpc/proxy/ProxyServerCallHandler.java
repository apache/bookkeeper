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

package org.apache.bookkeeper.common.grpc.proxy;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;

/**
 * Abstract server call handler.
 */
class ProxyServerCallHandler<ReqT, RespT> implements ServerCallHandler<ReqT, RespT> {

    private final ChannelFinder finder;
    private final CallOptions callOptions;

    ProxyServerCallHandler(ChannelFinder finder,
                           CallOptions callOptions) {
        this.finder = finder;
        this.callOptions = callOptions;
    }

    @Override
    public Listener<ReqT> startCall(ServerCall<ReqT, RespT> serverCall, Metadata headers) {
        Channel channel = finder.findChannel(serverCall, headers);
        ClientCall<ReqT, RespT> clientCall = channel.newCall(
            serverCall.getMethodDescriptor(), callOptions);
        ProxyCall<ReqT, RespT> proxyCall = new ProxyCall<>(
            serverCall,
            clientCall);
        clientCall.start(proxyCall.getClientCallListener(), headers);
        serverCall.request(1);
        clientCall.request(1);
        return proxyCall.getServerCallListener();
    }
}
