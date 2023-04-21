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

import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Status.Code;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Client side monitoring for grpc services.
 */
class ServerStats {

    private final Counter rpcStarted;
    private final Counter rpcCompleted;
    private final Counter streamMessagesReceived;
    private final Counter streamMessagesSent;
    private final Optional<OpStatsLogger> completedLatencyMicros;

    private ServerStats(StatsLogger rootStatsLogger,
                        boolean includeLatencyHistograms,
                        boolean streamRequests,
                        boolean streamResponses) {
        this.rpcStarted = rootStatsLogger.getCounter("grpc_started");
        this.rpcCompleted = rootStatsLogger.getCounter("grpc_completed");
        if (streamRequests) {
            this.streamMessagesReceived = rootStatsLogger.getCounter("grpc_msg_received");
        } else {
            this.streamMessagesReceived = NullStatsLogger.INSTANCE.getCounter("grpc_msg_received");
        }
        if (streamResponses) {
            this.streamMessagesSent = rootStatsLogger.getCounter("grpc_msg_sent");
        } else {
            this.streamMessagesSent = NullStatsLogger.INSTANCE.getCounter("grpc_msg_sent");
        }
        if (includeLatencyHistograms) {
            this.completedLatencyMicros = Optional.of(
                rootStatsLogger.getOpStatsLogger("grpc_latency_micros")
            );
        } else {
            this.completedLatencyMicros = Optional.empty();
        }
    }

    public void recordCallStarted() {
        rpcStarted.inc();
    }

    public void recordServerHandled(Code code) {
        rpcCompleted.inc();
    }

    public void recordStreamMessageSent() {
        streamMessagesSent.inc();
    }

    public void recordStreamMessageReceived() {
        streamMessagesReceived.inc();
    }

    public boolean shouldRecordLatency() {
        return completedLatencyMicros.isPresent();
    }

    public void recordLatency(boolean success, long latencyMicros) {
        completedLatencyMicros.ifPresent(latencyLogger -> {
            if (success) {
                latencyLogger.registerSuccessfulEvent(latencyMicros, TimeUnit.MICROSECONDS);
            } else {
                latencyLogger.registerFailedEvent(latencyMicros, TimeUnit.MICROSECONDS);
            }
        });
    }

    /**
     * Knows how to produce {@link ServerStats} instances for individual methods.
     */
    static class Factory {

        private final boolean includeLatencyHistograms;

        Factory(boolean includeLatencyHistograms) {
            this.includeLatencyHistograms = includeLatencyHistograms;
        }

        /** Creates a {@link ServerStats} for the supplied method. */
        <ReqT, RespT> ServerStats createMetricsForMethod(MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                         StatsLogger statsLogger) {

            String fullMethodName = methodDescriptor.getFullMethodName();
            String serviceName = MethodDescriptor.extractFullServiceName(fullMethodName);
            String methodName = fullMethodName.substring(serviceName.length() + 1);

            MethodType type = methodDescriptor.getType();
            return new ServerStats(
                statsLogger.scope(methodName),
                includeLatencyHistograms,
                type == MethodType.CLIENT_STREAMING || type == MethodType.BIDI_STREAMING,
                type == MethodType.SERVER_STREAMING || type == MethodType.BIDI_STREAMING);
        }
    }

}
