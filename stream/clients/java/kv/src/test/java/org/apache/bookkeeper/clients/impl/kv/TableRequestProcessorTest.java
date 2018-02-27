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

import static org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest.RequestCase.KV_DELETE_REQ;
import static org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest.RequestCase.KV_INCR_REQ;
import static org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest.RequestCase.KV_PUT_REQ;
import static org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest.RequestCase.KV_RANGE_REQ;
import static org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest.RequestCase.KV_TXN_REQ;
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
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest.RequestCase;
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
        testProcess(KV_RANGE_REQ);
    }

    @Test
    public void testProcessPutRequest() throws Exception {
        testProcess(KV_PUT_REQ);
    }

    @Test
    public void testProcessDeleteRequest() throws Exception {
        testProcess(KV_DELETE_REQ);
    }

    @Test
    public void testProcessIncrementRequest() throws Exception {
        testProcess(KV_INCR_REQ);
    }

    @Test
    public void testProcessTxnRequest() throws Exception {
        testProcess(KV_TXN_REQ);
    }

    private void testProcess(RequestCase type) throws Exception {
        @Cleanup("shutdown") ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        StorageContainerChannel scChannel = mock(StorageContainerChannel.class);

        CompletableFuture<StorageServerChannel> serverChannelFuture = FutureUtils.createFuture();
        when(scChannel.getStorageContainerChannelFuture()).thenReturn(serverChannelFuture);

        StorageContainerResponse.Builder respBuilder = StorageContainerResponse.newBuilder()
            .setCode(StatusCode.SUCCESS);

        switch (type) {
            case KV_PUT_REQ:
                respBuilder.setKvPutResp(PutResponse.newBuilder().build());
                break;
            case KV_DELETE_REQ:
                respBuilder.setKvDeleteResp(DeleteRangeResponse.newBuilder().build());
                break;
            case KV_RANGE_REQ:
                respBuilder.setKvRangeResp(RangeResponse.newBuilder().build());
                break;
            case KV_INCR_REQ:
                respBuilder.setKvIncrResp(IncrementResponse.newBuilder().build());
                break;
            case KV_TXN_REQ:
                respBuilder.setKvTxnResp(TxnResponse.newBuilder().build());
                break;
            default:
                break;
        }
        StorageContainerResponse response = respBuilder.build();

        AtomicReference<StorageContainerRequest> receivedRequest = new AtomicReference<>(null);
        AtomicReference<RequestCase> receivedRequestType = new AtomicReference<>(null);
        TableServiceImplBase tableService = new TableServiceImplBase() {
            @Override
            public void range(StorageContainerRequest request,
                              StreamObserver<StorageContainerResponse> responseObserver) {
                receivedRequest.set(request);
                receivedRequestType.set(KV_RANGE_REQ);
                complete(responseObserver);
            }

            @Override
            public void put(StorageContainerRequest request,
                            StreamObserver<StorageContainerResponse> responseObserver) {
                receivedRequest.set(request);
                receivedRequestType.set(KV_PUT_REQ);
                complete(responseObserver);
            }

            @Override
            public void delete(StorageContainerRequest request,
                               StreamObserver<StorageContainerResponse> responseObserver) {
                receivedRequest.set(request);
                receivedRequestType.set(KV_DELETE_REQ);
                complete(responseObserver);
            }

            @Override
            public void txn(StorageContainerRequest request,
                            StreamObserver<StorageContainerResponse> responseObserver) {
                receivedRequest.set(request);
                receivedRequestType.set(KV_TXN_REQ);
                complete(responseObserver);
            }

            @Override
            public void increment(StorageContainerRequest request,
                                  StreamObserver<StorageContainerResponse> responseObserver) {
                receivedRequest.set(request);
                receivedRequestType.set(KV_INCR_REQ);
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

        StorageContainerRequest.Builder requestBuilder = StorageContainerRequest.newBuilder();
        switch (type) {
            case KV_PUT_REQ:
                requestBuilder.setKvPutReq(PutRequest.newBuilder().build());
                break;
            case KV_DELETE_REQ:
                requestBuilder.setKvDeleteReq(DeleteRangeRequest.newBuilder().build());
                break;
            case KV_RANGE_REQ:
                requestBuilder.setKvRangeReq(RangeRequest.newBuilder().build());
                break;
            case KV_INCR_REQ:
                requestBuilder.setKvIncrReq(IncrementRequest.newBuilder().build());
                break;
            case KV_TXN_REQ:
                requestBuilder.setKvTxnReq(TxnRequest.newBuilder().build());
                break;
            default:
                break;
        }
        StorageContainerRequest request = requestBuilder.build();

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
