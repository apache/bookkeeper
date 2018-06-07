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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.clients.utils.ClientConstants;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.ResponseHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.TableServiceImplBase;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.junit.Test;

/**
 * Unit test of {@link IncrementRequestProcessor}.
 */
public class IncrementRequestProcessorTest extends GrpcClientTestBase {

    @Override
    protected void doSetup() throws Exception {
    }

    @Override
    protected void doTeardown() throws Exception {
    }

    protected IncrementResponse newSuccessResponse() {
        return IncrementResponse.newBuilder()
            .setHeader(ResponseHeader.newBuilder()
                .setCode(StatusCode.SUCCESS)
                .build())
            .build();
    }

    protected IncrementRequest newRequest() {
        return IncrementRequest.newBuilder()
            .build();
    }

    @Test
    public void testProcess() throws Exception {
        @Cleanup("shutdown") ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        StorageContainerChannel scChannel = mock(StorageContainerChannel.class);

        CompletableFuture<StorageServerChannel> serverChannelFuture = FutureUtils.createFuture();
        when(scChannel.getStorageContainerChannelFuture()).thenReturn(serverChannelFuture);

        IncrementResponse response = newSuccessResponse();

        AtomicReference<Object> receivedRequest = new AtomicReference<>(null);
        TableServiceImplBase tableService = new TableServiceImplBase() {

            @Override
            public void increment(IncrementRequest request,
                                  StreamObserver<IncrementResponse> responseObserver) {
                receivedRequest.set(request);
                complete(responseObserver);
            }

            private void complete(StreamObserver<IncrementResponse> responseStreamObserver) {
                responseStreamObserver.onNext(response);
                responseStreamObserver.onCompleted();
            }
        };
        serviceRegistry.addService(tableService.bindService());
        StorageServerChannel ssChannel = new StorageServerChannel(
            InProcessChannelBuilder.forName(serverName).directExecutor().build(),
            Optional.empty());
        serverChannelFuture.complete(ssChannel);

        IncrementRequest request = newRequest();

        IncrementRequestProcessor<String> processor = IncrementRequestProcessor.of(
            request,
            resp -> "test",
            scChannel,
            scheduler,
            ClientConstants.DEFAULT_INFINIT_BACKOFF_POLICY);
        assertEquals("test", FutureUtils.result(processor.process()));
        assertSame(request, receivedRequest.get());
    }

}
