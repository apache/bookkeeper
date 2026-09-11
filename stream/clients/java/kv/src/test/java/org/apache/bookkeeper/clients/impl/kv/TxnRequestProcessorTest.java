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
package org.apache.bookkeeper.clients.impl.kv;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.utils.ClientConstants;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.TableServiceImplBase;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.junit.Test;

/**
 * Unit test of {@link TxnRequestProcessor}.
 */
public class TxnRequestProcessorTest extends GrpcClientTestBase {

    @Override
    protected void doSetup() throws Exception {
    }

    @Override
    protected void doTeardown() throws Exception {
    }

    protected TxnResponse newSuccessResponse() {
        TxnResponse resp = new TxnResponse();
        resp.setHeader().setCode(StatusCode.SUCCESS);
        return resp;
    }

    protected TxnRequest newRequest() {
        return new TxnRequest();
    }

    @Test
    public void testProcess() throws Exception {
        @Cleanup("shutdown") ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        StorageContainerChannel scChannel = mock(StorageContainerChannel.class);

        CompletableFuture<StorageServerChannel> serverChannelFuture = FutureUtils.createFuture();
        when(scChannel.getStorageContainerChannelFuture()).thenReturn(serverChannelFuture);

        TxnResponse response = newSuccessResponse();

        AtomicReference<Object> receivedRequest = new AtomicReference<>(null);
        TableServiceImplBase tableService = new TableServiceImplBase() {

            @Override
            public void txn(TxnRequest request,
                            StreamObserver<TxnResponse> responseObserver) {
                receivedRequest.set(request);
                complete(responseObserver);
            }

            private void complete(StreamObserver<TxnResponse> responseStreamObserver) {
                responseStreamObserver.onNext(response);
                responseStreamObserver.onCompleted();
            }
        };
        serviceRegistry.addService(tableService.bindService());
        StorageServerChannel ssChannel = new StorageServerChannel(
            InProcessChannelBuilder.forName(serverName).directExecutor().build(),
            Optional.empty());
        serverChannelFuture.complete(ssChannel);

        TxnRequest request = newRequest();

        TxnRequestProcessor<String> processor = TxnRequestProcessor.of(
            () -> request,
            resp -> "test",
            scChannel,
            scheduler,
            ClientConstants.DEFAULT_INFINIT_BACKOFF_POLICY);
        assertEquals("test", FutureUtils.result(processor.process()));
        assertEquals(request, receivedRequest.get());
    }

    /**
     * Serializing a request drains the ByteBuf slices stored in it, so a request instance must
     * not be reused across RPC attempts. Verify that a retried txn sends an intact request built
     * fresh from the request supplier for every attempt.
     */
    @Test
    public void testRequestRebuiltForEachAttempt() throws Exception {
        @Cleanup("shutdown") ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        StorageContainerChannel scChannel = mock(StorageContainerChannel.class);

        CompletableFuture<StorageServerChannel> serverChannelFuture = FutureUtils.createFuture();
        when(scChannel.getStorageContainerChannelFuture()).thenReturn(serverChannelFuture);

        TxnResponse response = newSuccessResponse();

        List<TxnRequest> receivedRequests = new CopyOnWriteArrayList<>();
        TableServiceImplBase tableService = new TableServiceImplBase() {

            @Override
            public void txn(TxnRequest request,
                            StreamObserver<TxnResponse> responseObserver) {
                receivedRequests.add(request);
                if (receivedRequests.size() == 1) {
                    // fail the first attempt with a retryable status to force a retry
                    responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
                } else {
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            }
        };
        serviceRegistry.addService(tableService.bindService());
        StorageServerChannel ssChannel = new StorageServerChannel(
            InProcessChannelBuilder.forName(serverName).directExecutor().build(),
            Optional.empty());
        serverChannelFuture.complete(ssChannel);

        ByteBuf key = Unpooled.wrappedBuffer("txn-key".getBytes(UTF_8));
        ByteBuf value = Unpooled.wrappedBuffer("txn-value".getBytes(UTF_8));
        Supplier<TxnRequest> requestSupplier = () -> {
            TxnRequest request = new TxnRequest();
            request.addCompare().setKey(key.slice());
            request.addSuccess().setRequestPut()
                .setKey(key.slice())
                .setValue(value.slice());
            request.setHeader()
                .setStreamId(1234L)
                .setRKey(key.slice());
            return request;
        };

        TxnRequestProcessor<String> processor = TxnRequestProcessor.of(
            requestSupplier,
            resp -> "test",
            scChannel,
            scheduler,
            ClientConstants.DEFAULT_INFINIT_BACKOFF_POLICY);
        assertEquals("test", FutureUtils.result(processor.process()));

        assertEquals(2, receivedRequests.size());
        for (TxnRequest received : receivedRequests) {
            assertEquals(1, received.getComparesCount());
            assertArrayEquals("txn-key".getBytes(UTF_8), received.getCompareAt(0).getKey());
            assertEquals(1, received.getSuccessesCount());
            assertArrayEquals("txn-key".getBytes(UTF_8), received.getSuccessAt(0).getRequestPut().getKey());
            assertArrayEquals("txn-value".getBytes(UTF_8), received.getSuccessAt(0).getRequestPut().getValue());
            assertArrayEquals("txn-key".getBytes(UTF_8), received.getHeader().getRKey());
        }
    }

}
