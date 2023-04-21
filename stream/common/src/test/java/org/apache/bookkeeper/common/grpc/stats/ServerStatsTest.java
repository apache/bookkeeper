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

import static org.junit.Assert.assertEquals;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.grpc.stats.ServerStats.Factory;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestOpStatsLogger;
import org.bookkeeper.tests.proto.rpc.PingPongServiceGrpc;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link ServerStats}.
 */
public class ServerStatsTest {

    private Factory factoryWithHistograms;
    private Factory factoryWithoutHistograms;
    private TestStatsProvider statsProvider;

    @Before
    public void setup() {
        this.statsProvider = new TestStatsProvider();
        this.factoryWithHistograms = new Factory(true);
        this.factoryWithoutHistograms = new Factory(false);
    }

    @Test
    public void testServerStatsWithHistogram() {
        testServerStats(factoryWithHistograms, true);
    }

    @Test
    public void testServerStatsWithoutHistogram() {
        testServerStats(factoryWithoutHistograms, false);
    }

    private void testServerStats(Factory clientStatsFactory,
                                 boolean includeLatencyHistogram) {
        // test unary method
        MethodDescriptor<?, ?> unaryMethod = PingPongServiceGrpc.getPingPongMethod();
        testServerStats(
            clientStatsFactory,
            includeLatencyHistogram,
            unaryMethod,
            "PingPong",
            "unary",
            1,
            1,
            0,
            0
        );
        // test client streaming
        MethodDescriptor<?, ?> clientStreamingMethod = PingPongServiceGrpc.getLotsOfPingsMethod();
        testServerStats(
            clientStatsFactory,
            includeLatencyHistogram,
            clientStreamingMethod,
            "LotsOfPings",
            "client_streaming",
            1,
            1,
            0,
            2
        );
        // test server streaming
        MethodDescriptor<?, ?> serverStreamingMethod = PingPongServiceGrpc.getLotsOfPongsMethod();
        testServerStats(
            clientStatsFactory,
            includeLatencyHistogram,
            serverStreamingMethod,
            "LotsOfPongs",
            "server_streaming",
            1,
            1,
            1,
            0
        );
        // test server streaming
        MethodDescriptor<?, ?> biStreamingMethod = PingPongServiceGrpc.getBidiPingPongMethod();
        testServerStats(
            clientStatsFactory,
            includeLatencyHistogram,
            biStreamingMethod,
            "BidiPingPong",
            "bidi_streaming",
            1,
            1,
            1,
            2
        );
    }

    private void testServerStats(Factory clientStatsFactory,
                                 boolean includeLatencyHistogram,
                                 MethodDescriptor<?, ?> method,
                                 String methodName,
                                 String statsScope,
                                 long expectedCallStarted,
                                 long expectedCallCompleted,
                                 long expectedStreamMsgsSent,
                                 long expectedStreamMsgsReceived) {
        StatsLogger statsLogger = statsProvider.getStatsLogger(statsScope);
        ServerStats unaryStats = clientStatsFactory.createMetricsForMethod(
            method,
            statsLogger
        );
        unaryStats.recordCallStarted();
        assertEquals(
            expectedCallStarted,
            statsLogger.scope(methodName).getCounter("grpc_started").get().longValue());
        unaryStats.recordServerHandled(Status.OK.getCode());
        assertEquals(
            expectedCallCompleted,
            statsLogger.scope(methodName).getCounter("grpc_completed").get().longValue());
        unaryStats.recordStreamMessageSent();
        assertEquals(
            expectedStreamMsgsSent,
            statsLogger.scope(methodName).getCounter("grpc_msg_sent").get().longValue());
        unaryStats.recordStreamMessageReceived();
        unaryStats.recordStreamMessageReceived();
        assertEquals(
            expectedStreamMsgsReceived,
            statsLogger.scope(methodName).getCounter("grpc_msg_received").get().longValue());
        long latencyMicros = 12345L;
        unaryStats.recordLatency(true, latencyMicros);
        TestOpStatsLogger opStatsLogger =
            (TestOpStatsLogger) statsLogger.scope(methodName).getOpStatsLogger("grpc_latency_micros");
        if (includeLatencyHistogram) {
            assertEquals(1, opStatsLogger.getSuccessCount());
            assertEquals(
                TimeUnit.MICROSECONDS.toNanos(latencyMicros),
                (long) opStatsLogger.getSuccessAverage());
        } else {
            assertEquals(0, opStatsLogger.getSuccessCount());
            assertEquals(0, (long) opStatsLogger.getSuccessAverage());
        }
    }

}
