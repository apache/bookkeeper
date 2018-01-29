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
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest.Type;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerResponse;
import org.apache.bookkeeper.stream.proto.storage.TableServiceGrpc.TableServiceImplBase;
import org.junit.Test;

/**
 * Unit test of {@link TableRequestProcessor}.
 */
public class TableRequestProcessorTest extends GrpcClientTestBase {

    @Override
    protected void doSetup() throws Exception {
    }

    @Override
    protected void doTeardown() throws Exception {
    }

    @Test
    public void testProcessRangeRequest() throws Exception {
        testProcess(Type.KV_RANGE);
    }

    @Test
    public void testProcessPutRequest() throws Exception {
        testProcess(Type.KV_PUT);
    }

    @Test
    public void testProcessDeleteRequest() throws Exception {
        testProcess(Type.KV_DELETE);
    }

    @Test
    public void testProcessIncrementRequest() throws Exception {
        testProcess(Type.KV_INCREMENT);
    }

    @Test
    public void testProcessTxnRequest() throws Exception {
        testProcess(Type.KV_TXN);
    }

    private void testProcess(Type type) throws Exception {
        @Cleanup("shutdown") ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        StorageContainerChannel scChannel = mock(StorageContainerChannel.class);

        CompletableFuture<StorageServerChannel> serverChannelFuture = FutureUtils.createFuture();
        when(scChannel.getStorageContainerChannelFuture()).thenReturn(serverChannelFuture);

        StorageContainerResponse.Builder respBuilder = StorageContainerResponse.newBuilder()
            .setCode(StatusCode.SUCCESS);

        switch (type) {
            case KV_PUT:
                respBuilder.setKvPutResp(PutResponse.newBuilder().build());
                break;
            case KV_DELETE:
                respBuilder.setKvDeleteResp(DeleteRangeResponse.newBuilder().build());
                break;
            case KV_RANGE:
                respBuilder.setKvRangeResp(RangeResponse.newBuilder().build());
                break;
            case KV_INCREMENT:
                respBuilder.setKvIncrResp(IncrementResponse.newBuilder().build());
                break;
            case KV_TXN:
                respBuilder.setKvTxnResp(TxnResponse.newBuilder().build());
                break;
            default:
                break;
        }
        StorageContainerResponse response = respBuilder.build();

        AtomicReference<StorageContainerRequest> receivedRequest = new AtomicReference<>(null);
        AtomicReference<Type> receivedRequestType = new AtomicReference<>(null);
        TableServiceImplBase tableService = new TableServiceImplBase() {
            @Override
            public void range(StorageContainerRequest request,
                              StreamObserver<StorageContainerResponse> responseObserver) {
                receivedRequest.set(request);
                receivedRequestType.set(Type.KV_RANGE);
                complete(responseObserver);
            }

            @Override
            public void put(StorageContainerRequest request,
                            StreamObserver<StorageContainerResponse> responseObserver) {
                receivedRequest.set(request);
                receivedRequestType.set(Type.KV_PUT);
                complete(responseObserver);
            }

            @Override
            public void delete(StorageContainerRequest request,
                               StreamObserver<StorageContainerResponse> responseObserver) {
                receivedRequest.set(request);
                receivedRequestType.set(Type.KV_DELETE);
                complete(responseObserver);
            }

            @Override
            public void txn(StorageContainerRequest request,
                            StreamObserver<StorageContainerResponse> responseObserver) {
                receivedRequest.set(request);
                receivedRequestType.set(Type.KV_TXN);
                complete(responseObserver);
            }

            @Override
            public void increment(StorageContainerRequest request,
                                  StreamObserver<StorageContainerResponse> responseObserver) {
                receivedRequest.set(request);
                receivedRequestType.set(Type.KV_INCREMENT);
                complete(responseObserver);
            }

            private void complete(StreamObserver<StorageContainerResponse> responseStreamObserver) {
                responseStreamObserver.onNext(response);
                responseStreamObserver.onCompleted();
            }
        };
        serviceRegistry.addService(tableService.bindService());
        StorageServerChannel ssChannel = new StorageServerChannel(
            InProcessChannelBuilder.forName(serverName).directExecutor().build(),
            Optional.empty());
        serverChannelFuture.complete(ssChannel);

        StorageContainerRequest request = StorageContainerRequest.newBuilder()
            .setType(type)
            .build();
        TableRequestProcessor<String> processor = TableRequestProcessor.of(
            request,
            resp -> "test",
            scChannel,
            scheduler);
        assertEquals("test", FutureUtils.result(processor.process()));
        assertSame(request, receivedRequest.get());
        assertEquals(type, receivedRequestType.get());
    }

}
