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

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;
import org.apache.bookkeeper.common.util.MathUtils;

/**
 * A {@link SimpleForwardingServerCall} which increments counters for rpc calls.
 */
class MonitoringServerCall<ReqT, RespT> extends SimpleForwardingServerCall<ReqT, RespT> {

    private final ServerStats stats;
    private final long startNanos;

    MonitoringServerCall(ServerCall<ReqT, RespT> delegate,
                         ServerStats stats) {
        super(delegate);
        this.stats = stats;
        this.startNanos = MathUtils.nowInNano();
        stats.recordCallStarted();
    }

    @Override
    public void sendMessage(RespT message) {
        stats.recordStreamMessageSent();
        super.sendMessage(message);
    }

    @Override
    public void close(Status status, Metadata trailers) {
        stats.recordServerHandled(status.getCode());
        if (stats.shouldRecordLatency()) {
            long latencyMicros = MathUtils.elapsedMicroSec(startNanos);
            stats.recordLatency(Status.OK == status, latencyMicros);
        }
        super.close(status, trailers);
    }
}
