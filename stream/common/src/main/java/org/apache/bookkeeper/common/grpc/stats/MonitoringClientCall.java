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

package org.apache.bookkeeper.common.grpc.stats;

import io.grpc.ClientCall;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;

/**
 * A {@link SimpleForwardingClientCall} which increments counters for rpc calls.
 */
class MonitoringClientCall<ReqT, RespT> extends SimpleForwardingClientCall<ReqT, RespT> {

    private final ClientStats stats;

    MonitoringClientCall(ClientCall<ReqT, RespT> delegate,
                         ClientStats stats) {
        super(delegate);
        this.stats = stats;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
        stats.recordCallStarted();
        super.start(new MonitoringClientCallListener<>(
            responseListener, stats
        ), headers);
    }

    @Override
    public void sendMessage(ReqT message) {
        stats.recordStreamMessageSent();
        super.sendMessage(message);
    }
}
