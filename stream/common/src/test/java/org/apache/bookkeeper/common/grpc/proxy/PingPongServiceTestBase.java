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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import lombok.Data;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.ExceptionalFunction;
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
 * Test grpc reverse proxy using {@link org.apache.bookkeeper.tests.rpc.PingPongService}.
 */
public abstract class PingPongServiceTestBase {

    private static final int NUM_PONGS_PER_PING = 10;
    private static final String SERVICE_NAME = "pingpong";

    private final boolean useReverseProxy;

    protected Server realServer;
    protected Server proxyServer;
    protected PingPongService service;
    protected ManagedChannel proxyChannel;
    protected ManagedChannel clientChannel;
    protected PingPongServiceStub client;

    PingPongServiceTestBase(boolean useReverseProxy) {
        this.useReverseProxy = useReverseProxy;
    }

    @Before
    public void setup() throws Exception {
        service = new PingPongService(NUM_PONGS_PER_PING);
        ServerServiceDefinition pingPongServiceDef = service.bindService();

        String serverName;
        if (useReverseProxy) {
            serverName = "proxy-" + SERVICE_NAME;
        } else {
            serverName = SERVICE_NAME;
        }
        // build a real server
        MutableHandlerRegistry realRegistry = new MutableHandlerRegistry();
        realServer = InProcessServerBuilder
            .forName(serverName)
            .fallbackHandlerRegistry(realRegistry)
            .directExecutor()
            .build()
            .start();
        realRegistry.addService(pingPongServiceDef);

        if (useReverseProxy) {
            proxyChannel = InProcessChannelBuilder.forName(serverName)
                .usePlaintext()
                .build();

            ProxyHandlerRegistry registry = ProxyHandlerRegistry.newBuilder()
                .addService(pingPongServiceDef)
                .setChannelFinder((serverCall, header) -> proxyChannel)
                .build();
            proxyServer = InProcessServerBuilder
                .forName(SERVICE_NAME)
                .fallbackHandlerRegistry(registry)
                .directExecutor()
                .build()
                .start();
        } else {
            proxyServer = realServer;
        }

        clientChannel = InProcessChannelBuilder.forName(SERVICE_NAME)
            .usePlaintext()
            .build();

        client = PingPongServiceGrpc.newStub(clientChannel);

    }

    @After
    public void teardown() throws Exception {
        if (null != clientChannel) {
            clientChannel.shutdown();
        }

        if (null != proxyServer) {
            proxyServer.shutdown();
        }

        if (null != proxyChannel) {
            proxyChannel.shutdown();
        }

        if (null != realServer && proxyServer != realServer) {
            realServer.shutdown();
        }
    }


    @Test
    public void testUnary() {
        PingPongServiceBlockingStub clientBlocking = PingPongServiceGrpc.newBlockingStub(clientChannel);

        long sequence = ThreadLocalRandom.current().nextLong();
        PingRequest request = PingRequest.newBuilder()
            .setSequence(sequence)
            .build();
        PongResponse response = clientBlocking.pingPong(request);
        assertEquals(sequence, response.getLastSequence());
        assertEquals(1, response.getNumPingReceived());
        assertEquals(0, response.getSlotId());
    }

    @Test
    public void testServerStreaming() {
        PingPongServiceBlockingStub clientBlocking = PingPongServiceGrpc.newBlockingStub(clientChannel);

        long sequence = ThreadLocalRandom.current().nextLong(100000);
        PingRequest request = PingRequest.newBuilder()
            .setSequence(sequence)
            .build();
        Iterator<PongResponse> respIter = clientBlocking.lotsOfPongs(request);
        int count = 0;
        while (respIter.hasNext()) {
            PongResponse resp = respIter.next();
            assertEquals(sequence, resp.getLastSequence());
            assertEquals(1, resp.getNumPingReceived());
            assertEquals(count, resp.getSlotId());
            ++count;
        }
    }

    @Test
    public void testClientStreaming() throws Exception {
        final int numPings = 100;
        final long sequence = ThreadLocalRandom.current().nextLong(100000);
        final CompletableFuture<Void> respFuture = new CompletableFuture<>();
        final LinkedBlockingQueue<PongResponse> respQueue = new LinkedBlockingQueue<>();
        StreamObserver<PingRequest> pinger = client.lotsOfPings(new StreamObserver<PongResponse>() {
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
    }

    @Test
    public void testBidiStreaming() throws Exception {
        final int numPings = 100;

        final CompletableFuture<Void> respFuture = new CompletableFuture<>();
        final LinkedBlockingQueue<PongResponse> respQueue = new LinkedBlockingQueue<>();
        StreamObserver<PingRequest> pinger = client.bidiPingPong(new StreamObserver<PongResponse>() {
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
    }

    @Data
    static class Runner implements Runnable {

        private final CountDownLatch startLatch;
        private final CountDownLatch doneLatch;
        private final AtomicReference<Exception> exceptionHolder;
        private final ExceptionalFunction<Void, Void> func;

        @Override
        public void run() {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
            }
            int numIters = ThreadLocalRandom.current().nextInt(10, 100);
            IntStream.of(numIters).forEach(idx -> {
                if (null != exceptionHolder.get()) {
                    // break if exception occurs
                    return;
                }
                try {
                    func.apply(null);
                } catch (Exception e) {
                    exceptionHolder.set(e);
                    doneLatch.countDown();
                }
            });
            if (null == exceptionHolder.get()) {
                doneLatch.countDown();
            }
        }
    }

    @Test
    public void testMixed() throws Exception {
        int numTypes = 4;

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(numTypes);
        final AtomicReference<Exception> exception = new AtomicReference<>();

        ExecutorService executor = Executors.newFixedThreadPool(numTypes);
        // start unmary test
        executor.submit(new Runner(startLatch, doneLatch, exception, ignored -> {
            testUnary();
            return null;
        }));

        // start client streaming tests
        executor.submit(new Runner(startLatch, doneLatch, exception, ignored -> {
            testClientStreaming();
            return null;
        }));

        // start server streaming tests
        executor.submit(new Runner(startLatch, doneLatch, exception, ignored -> {
            testServerStreaming();
            return null;
        }));

        // start bidi streaming tests
        executor.submit(new Runner(startLatch, doneLatch, exception, ignored -> {
            testBidiStreaming();
            return null;
        }));

        // start the tests
        startLatch.countDown();

        // wait for tests to complete
        doneLatch.await();

        // make sure all succeed
        assertNull("Exception found : " + exception.get(), exception.get());
    }

}
