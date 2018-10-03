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

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.common.grpc.stats.ServerStats.Factory;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@link ServerInterceptor} that sends stats about grpc calls to stats logger.
 */
public class MonitoringServerInterceptor implements ServerInterceptor {

    /**
     * Create a monitoring client interceptor with provided stats logger and configuration.
     *
     * @param statsLogger stats logger to collect grpc stats
     * @param includeLatencyHistograms flag indicates whether to include latency histograms or not
     * @return a monitoring client interceptor
     */
    public static MonitoringServerInterceptor create(StatsLogger statsLogger,
                                                     boolean includeLatencyHistograms) {
        return new MonitoringServerInterceptor(
            new Factory(includeLatencyHistograms), statsLogger);
    }

    private final Factory statsFactory;
    private final StatsLogger statsLogger;
    private final ConcurrentMap<String, ServerStats> methods;

    private MonitoringServerInterceptor(Factory statsFactory,
                                        StatsLogger statsLogger) {
        this.statsFactory = statsFactory;
        this.statsLogger = statsLogger;
        this.methods = new ConcurrentHashMap<>();
    }

    private ServerStats getMethodStats(MethodDescriptor<?, ?> method) {
        ServerStats stats = methods.get(method.getFullMethodName());
        if (null != stats) {
            return stats;
        }
        ServerStats newStats = statsFactory.createMetricsForMethod(method, statsLogger);
        ServerStats oldStats = methods.putIfAbsent(method.getFullMethodName(), newStats);
        if (null != oldStats) {
            return oldStats;
        } else {
            return newStats;
        }
    }


    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                      Metadata headers,
                                                      ServerCallHandler<ReqT, RespT> next) {
        MethodDescriptor<ReqT, RespT> method = call.getMethodDescriptor();
        ServerStats stats = getMethodStats(method);
        ServerCall<ReqT, RespT> monitoringCall = new MonitoringServerCall<>(call, stats);
        return new MonitoringServerCallListener<>(
            next.startCall(monitoringCall, headers), stats
        );
    }
}
