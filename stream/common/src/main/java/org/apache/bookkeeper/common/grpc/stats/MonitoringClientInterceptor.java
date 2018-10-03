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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.common.grpc.stats.ClientStats.Factory;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@link ClientInterceptor} that sends stats about grpc calls to stats logger.
 */
public class MonitoringClientInterceptor implements ClientInterceptor {

    /**
     * Create a monitoring client interceptor with provided stats logger and configuration.
     *
     * @param statsLogger stats logger to collect grpc stats
     * @param includeLatencyHistograms flag indicates whether to include latency histograms or not
     * @return a monitoring client interceptor
     */
    public static MonitoringClientInterceptor create(StatsLogger statsLogger,
                                                     boolean includeLatencyHistograms) {
        return new MonitoringClientInterceptor(
            new Factory(includeLatencyHistograms), statsLogger);
    }

    private final Factory statsFactory;
    private final StatsLogger statsLogger;
    private final ConcurrentMap<String, ClientStats> methods;

    private MonitoringClientInterceptor(Factory statsFactory,
                                        StatsLogger statsLogger) {
        this.statsFactory = statsFactory;
        this.statsLogger = statsLogger;
        this.methods = new ConcurrentHashMap<>();
    }

    private ClientStats getMethodStats(MethodDescriptor<?, ?> method) {
        ClientStats stats = methods.get(method.getFullMethodName());
        if (null != stats) {
            return stats;
        }
        ClientStats newStats = statsFactory.createMetricsForMethod(method, statsLogger);
        ClientStats oldStats = methods.putIfAbsent(method.getFullMethodName(), newStats);
        if (null != oldStats) {
            return oldStats;
        } else {
            return newStats;
        }
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        ClientStats stats = getMethodStats(method);
        return new MonitoringClientCall<>(
            next.newCall(method, callOptions),
            stats
        );
    }
}
