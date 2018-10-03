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

import io.grpc.ClientCall.Listener;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.Status;
import org.apache.bookkeeper.common.util.MathUtils;

/**
 * A {@link ForwardingClientCallListener} that monitors stats on grpc clients.
 */
class MonitoringClientCallListener<RespT> extends ForwardingClientCallListener<RespT> {

    private final Listener<RespT> delegate;
    private final ClientStats stats;
    private final long startNanos;

    MonitoringClientCallListener(Listener<RespT> delegate,
                                 ClientStats stats) {
        this.delegate = delegate;
        this.stats = stats;
        this.startNanos = MathUtils.nowInNano();
    }

    @Override
    protected Listener<RespT> delegate() {
        return delegate;
    }

    @Override
    public void onMessage(RespT message) {
        stats.recordStreamMessageReceived();
        super.onMessage(message);
    }

    @Override
    public void onClose(Status status, Metadata trailers) {
        stats.recordClientHandled(status.getCode());
        if (stats.shouldRecordLatency()) {
            long latencyMicros = MathUtils.elapsedMicroSec(startNanos);
            stats.recordLatency(Status.OK == status, latencyMicros);
        }
        super.onClose(status, trailers);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
