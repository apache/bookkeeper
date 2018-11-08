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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestOpStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider.TestStatsLogger;
import org.apache.bookkeeper.tests.rpc.PingPongService;
import org.bookkeeper.tests.proto.rpc.PingPongServiceGrpc;
import org.bookkeeper.tests.proto.rpc.PingPongServiceGrpc.PingPongServiceBlockingStub;
import org.bookkeeper.tests.proto.rpc.PingPongServiceGrpc.PingPongServiceStub;
import org.bookkeeper.tests.proto.rpc.PingRequest;
import org.bookkeeper.tests.proto.rpc.PongResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end integration test on grpc stats.
 */
public class GrpcStatsIntegrationTest {

    private static final int NUM_PONGS_PER_PING = 10;
    private static final String SERVICE_NAME = "pingpong";

    private Server server;
    private PingPongService service;
    private ManagedChannel channel;
    private Channel monitoredChannel;
    private PingPongServiceBlockingStub client;
    private PingPongServiceStub clientNonBlocking;
    private TestStatsProvider statsProvider;
    private TestStatsLogger clientStatsLogger;
    private TestStatsLogger serverStatsLogger;


    @Before
    public void setup() throws Exception {
        statsProvider = new TestStatsProvider();
        clientStatsLogger = statsProvider.getStatsLogger("client");
        serverStatsLogger = statsProvider.getStatsLogger("server");
        service = new PingPongService(NUM_PONGS_PER_PING);
        ServerServiceDefinition monitoredService = ServerInterceptors.intercept(
            service,
            MonitoringServerInterceptor.create(serverStatsLogger, true)
        );
        MutableHandlerRegistry registry = new MutableHandlerRegistry();
        server = InProcessServerBuilder
            .forName(SERVICE_NAME)
            .fallbackHandlerRegistry(registry)
            .directExecutor()
            .build()
            .start();
        registry.addService(monitoredService);

        channel = InProcessChannelBuilder.forName(SERVICE_NAME)
            .usePlaintext()
            .build();
        monitoredChannel = ClientInterceptors.intercept(
            channel,
            MonitoringClientInterceptor.create(clientStatsLogger, true)
        );
        client = PingPongServiceGrpc.newBlockingStub(monitoredChannel);
        clientNonBlocking = PingPongServiceGrpc.newStub(monitoredChannel);
    }

    @After
    public void teardown() {
        if (null != channel) {
            channel.shutdown();
        }
        if (null != server) {
            server.shutdown();
        }
    }

    private void assertStats(String methodName,
                             long numCalls,
                             long numClientMsgSent,
                             long numClientMsgReceived,
                             long numServerMsgSent,
                             long numServerMsgReceived) {
        // client stats
        assertEquals(
            numCalls,
            clientStatsLogger.scope(methodName).getCounter("grpc_started").get().longValue()
        );
        assertEquals(
            numCalls,
            clientStatsLogger.scope(methodName).getCounter("grpc_completed").get().longValue()
        );
        assertEquals(
            numClientMsgSent,
            clientStatsLogger.scope(methodName).getCounter("grpc_msg_sent").get().longValue()
        );
        assertEquals(
            numClientMsgReceived,
            clientStatsLogger.scope(methodName).getCounter("grpc_msg_received").get().longValue()
        );
        TestOpStatsLogger opStatsLogger =
            (TestOpStatsLogger) clientStatsLogger.scope(methodName).getOpStatsLogger("grpc_latency_micros");
        assertEquals(
            numCalls,
            opStatsLogger.getSuccessCount()
        );
        // server stats
        assertEquals(
            numCalls,
            serverStatsLogger.scope(methodName).getCounter("grpc_started").get().longValue()
        );
        assertEquals(
            numCalls,
            serverStatsLogger.scope(methodName).getCounter("grpc_completed").get().longValue()
        );
        assertEquals(
            numServerMsgSent,
            serverStatsLogger.scope(methodName).getCounter("grpc_msg_sent").get().longValue()
        );
        assertEquals(
            numServerMsgReceived,
            serverStatsLogger.scope(methodName).getCounter("grpc_msg_received").get().longValue()
        );
        opStatsLogger =
            (TestOpStatsLogger) serverStatsLogger.scope(methodName).getOpStatsLogger("grpc_latency_micros");
        assertEquals(
            numCalls,
            opStatsLogger.getSuccessCount()
        );
    }

    @Test
    public void testUnary() {
        long sequence = ThreadLocalRandom.current().nextLong();
        PingRequest request = PingRequest.newBuilder()
            .setSequence(sequence)
            .build();
        PongResponse response = client.pingPong(request);
        assertEquals(sequence, response.getLastSequence());
        assertEquals(1, response.getNumPingReceived());
        assertEquals(0, response.getSlotId());

        // verify the stats
        assertStats(
            "PingPong",
            1,
            0,
            0,
            0,
            0);
    }

    @Test
    public void testServerStreaming() {
        long sequence = ThreadLocalRandom.current().nextLong(100000);
        PingRequest request = PingRequest.newBuilder()
            .setSequence(sequence)
            .build();
        Iterator<PongResponse> respIter = client.lotsOfPongs(request);
        int count = 0;
        while (respIter.hasNext()) {
            PongResponse resp = respIter.next();
            assertEquals(sequence, resp.getLastSequence());
            assertEquals(1, resp.getNumPingReceived());
            assertEquals(count, resp.getSlotId());
            ++count;
        }

        assertStats(
            "LotsOfPongs",
            1,
            0,
            NUM_PONGS_PER_PING,
            NUM_PONGS_PER_PING,
            0);
    }

    @Test
    public void testClientStreaming() throws Exception {
        final int numPings = 100;
        final long sequence = ThreadLocalRandom.current().nextLong(100000);
        final CompletableFuture<Void> respFuture = new CompletableFuture<>();
        final LinkedBlockingQueue<PongResponse> respQueue = new LinkedBlockingQueue<>();
        StreamObserver<PingRequest> pinger = clientNonBlocking.lotsOfPings(new StreamObserver<PongResponse>() {
            @Override
            public void onNext(PongResponse resp) {
                respQueue.offer(resp);
            }

            @Override
            public void onError(Throwable t) {
                respFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                FutureUtils.complete(respFuture, null);
            }
        });

        for (int i = 0; i < numPings; i++) {
            PingRequest request = PingRequest.newBuilder()
                .setSequence(sequence + i)
                .build();
            pinger.onNext(request);
        }
        pinger.onCompleted();

        // wait for response to be received.
        result(respFuture);

        assertEquals(1, respQueue.size());

        PongResponse resp = respQueue.take();
        assertEquals(sequence + numPings - 1, resp.getLastSequence());
        assertEquals(numPings, resp.getNumPingReceived());
        assertEquals(0, resp.getSlotId());

        assertStats(
            "LotsOfPings",
            1,
            numPings,
            0,
            0,
            numPings
        );
    }

    @Test
    public void testBidiStreaming() throws Exception {
        final int numPings = 100;

        final CompletableFuture<Void> respFuture = new CompletableFuture<>();
        final LinkedBlockingQueue<PongResponse> respQueue = new LinkedBlockingQueue<>();
        StreamObserver<PingRequest> pinger = clientNonBlocking.bidiPingPong(new StreamObserver<PongResponse>() {
            @Override
            public void onNext(PongResponse resp) {
                respQueue.offer(resp);
            }

            @Override
            public void onError(Throwable t) {
                respFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                FutureUtils.complete(respFuture, null);
            }
        });

        final LinkedBlockingQueue<PingRequest> reqQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < numPings; i++) {
            final long sequence = ThreadLocalRandom.current().nextLong(100000);
            PingRequest request = PingRequest.newBuilder()
                .setSequence(sequence)
                .build();
            reqQueue.put(request);
            pinger.onNext(request);
        }
        pinger.onCompleted();

        // wait for response to be received
        result(respFuture);

        assertEquals(numPings, respQueue.size());

        int count = 0;
        for (PingRequest request : reqQueue) {
            PongResponse response = respQueue.take();

            assertEquals(request.getSequence(), response.getLastSequence());
            assertEquals(++count, response.getNumPingReceived());
            assertEquals(0, response.getSlotId());
        }
        assertNull(respQueue.poll());
        assertEquals(numPings, count);

        assertStats(
            "BidiPingPong",
            1,
            numPings,
            numPings,
            numPings,
            numPings
        );
    }

}
