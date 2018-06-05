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
package org.apache.bookkeeper.stream.storage.impl.grpc;

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createCreateNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createCreateStreamRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createDeleteNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createDeleteStreamRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetNamespaceRequest;
import static org.apache.bookkeeper.stream.protocol.util.ProtoUtils.createGetStreamRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.NamespaceProperties;
import org.apache.bookkeeper.stream.proto.StreamName;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.CreateStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.DeleteStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceRequest;
import org.apache.bookkeeper.stream.proto.storage.GetNamespaceResponse;
import org.apache.bookkeeper.stream.proto.storage.GetStreamRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStreamResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.storage.api.metadata.RangeStoreService;
import org.apache.bookkeeper.stream.storage.exceptions.StorageException;
import org.junit.Test;

/**
 * Unit test for {@link GrpcRootRangeService}.
 */
public class TestGrpcRootRangeService {

    private static final long colId = 12345L;
    private static final String nsName = "test-namespace-name";
    private static final NamespaceConfiguration namespaceConf =
        NamespaceConfiguration.newBuilder()
            .setDefaultStreamConf(DEFAULT_STREAM_CONF)
            .build();
    private static final NamespaceProperties namespaceProps =
        NamespaceProperties.newBuilder()
            .setNamespaceId(colId)
            .setNamespaceName(nsName)
            .setDefaultStreamConf(namespaceConf.getDefaultStreamConf())
            .build();
    private static final String streamName = "test-stream-name";
    private static final StreamProperties streamProps =
        StreamProperties.newBuilder()
            .setStorageContainerId(1234L)
            .setStreamConf(DEFAULT_STREAM_CONF)
            .setStreamName(streamName)
            .setStreamId(1234L)
            .build();

    private static final Throwable CAUSE = new StorageException("test-grpc-root-range-service");

    //
    // Test Namespace API
    //

    @Test
    public void testCreateNamespaceSuccess() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        CreateNamespaceResponse createResp = CreateNamespaceResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setNsProps(namespaceProps)
            .build();
        CreateNamespaceRequest createReq = createCreateNamespaceRequest(nsName, namespaceConf);
        when(rangeService.createNamespace(createReq)).thenReturn(
            CompletableFuture.completedFuture(createResp));
        AtomicReference<CreateNamespaceResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<CreateNamespaceResponse> streamObserver = new StreamObserver<CreateNamespaceResponse>() {
            @Override
            public void onNext(CreateNamespaceResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.createNamespace(
            CreateNamespaceRequest.newBuilder()
                .setName(nsName)
                .setNsConf(namespaceConf)
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertTrue(createResp == resultHolder.get());
        verify(rangeService, times(1)).createNamespace(createReq);
    }

    @Test
    public void testCreateNamespaceFailure() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        CreateNamespaceResponse createResp = CreateNamespaceResponse.newBuilder()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
            .build();
        CreateNamespaceRequest createReq = createCreateNamespaceRequest(nsName, namespaceConf);
        when(rangeService.createNamespace(createReq)).thenReturn(
            FutureUtils.exception(CAUSE));
        AtomicReference<CreateNamespaceResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<CreateNamespaceResponse> streamObserver = new StreamObserver<CreateNamespaceResponse>() {
            @Override
            public void onNext(CreateNamespaceResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.createNamespace(
            CreateNamespaceRequest.newBuilder()
                .setName(nsName)
                .setNsConf(namespaceConf)
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertEquals(createResp, resultHolder.get());
        verify(rangeService, times(1)).createNamespace(createReq);
    }

    @Test
    public void testDeleteNamespaceSuccess() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        DeleteNamespaceResponse deleteResp = DeleteNamespaceResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .build();
        DeleteNamespaceRequest deleteReq = createDeleteNamespaceRequest(nsName);
        when(rangeService.deleteNamespace(deleteReq)).thenReturn(
            CompletableFuture.completedFuture(deleteResp));
        AtomicReference<DeleteNamespaceResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<DeleteNamespaceResponse> streamObserver = new StreamObserver<DeleteNamespaceResponse>() {
            @Override
            public void onNext(DeleteNamespaceResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.deleteNamespace(
            DeleteNamespaceRequest.newBuilder()
                .setName(nsName)
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertTrue(deleteResp == resultHolder.get());
        verify(rangeService, times(1)).deleteNamespace(deleteReq);
    }

    @Test
    public void testDeleteNamespaceFailure() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        DeleteNamespaceResponse deleteResp = DeleteNamespaceResponse.newBuilder()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
            .build();
        DeleteNamespaceRequest deleteReq = createDeleteNamespaceRequest(nsName);
        when(rangeService.deleteNamespace(deleteReq)).thenReturn(
            FutureUtils.exception(CAUSE));
        AtomicReference<DeleteNamespaceResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<DeleteNamespaceResponse> streamObserver = new StreamObserver<DeleteNamespaceResponse>() {
            @Override
            public void onNext(DeleteNamespaceResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.deleteNamespace(
            DeleteNamespaceRequest.newBuilder()
                .setName(nsName)
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertEquals(deleteResp, resultHolder.get());
        verify(rangeService, times(1)).deleteNamespace(deleteReq);
    }

    @Test
    public void testGetNamespaceSuccess() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        GetNamespaceResponse getResp = GetNamespaceResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setNsProps(namespaceProps)
            .build();
        GetNamespaceRequest getReq = createGetNamespaceRequest(nsName);
        when(rangeService.getNamespace(getReq)).thenReturn(
            CompletableFuture.completedFuture(getResp));
        AtomicReference<GetNamespaceResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<GetNamespaceResponse> streamObserver = new StreamObserver<GetNamespaceResponse>() {
            @Override
            public void onNext(GetNamespaceResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.getNamespace(
            GetNamespaceRequest.newBuilder()
                .setName(nsName)
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertTrue(getResp == resultHolder.get());
        verify(rangeService, times(1)).getNamespace(getReq);
    }

    @Test
    public void testGetNamespaceFailure() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        GetNamespaceResponse getResp = GetNamespaceResponse.newBuilder()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
            .build();
        GetNamespaceRequest getReq = createGetNamespaceRequest(nsName);
        when(rangeService.getNamespace(getReq)).thenReturn(
            FutureUtils.exception(CAUSE));
        AtomicReference<GetNamespaceResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<GetNamespaceResponse> streamObserver = new StreamObserver<GetNamespaceResponse>() {
            @Override
            public void onNext(GetNamespaceResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.getNamespace(
            GetNamespaceRequest.newBuilder()
                .setName(nsName)
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertEquals(getResp, resultHolder.get());
        verify(rangeService, times(1)).getNamespace(getReq);
    }

    //
    // Test Stream API
    //

    @Test
    public void testCreateStreamSuccess() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        CreateStreamResponse createResp = CreateStreamResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setStreamProps(streamProps)
            .build();
        CreateStreamRequest createReq = createCreateStreamRequest(nsName, streamName, DEFAULT_STREAM_CONF);
        when(rangeService.createStream(createReq)).thenReturn(
            CompletableFuture.completedFuture(createResp));
        AtomicReference<CreateStreamResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<CreateStreamResponse> streamObserver = new StreamObserver<CreateStreamResponse>() {
            @Override
            public void onNext(CreateStreamResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.createStream(
            CreateStreamRequest.newBuilder()
                .setNsName(nsName)
                .setName(streamName)
                .setStreamConf(DEFAULT_STREAM_CONF)
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertTrue(createResp == resultHolder.get());
        verify(rangeService, times(1)).createStream(createReq);
    }

    @Test
    public void testCreateStreamFailure() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        CreateStreamResponse createResp = CreateStreamResponse.newBuilder()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
            .build();
        CreateStreamRequest createReq = createCreateStreamRequest(nsName, streamName, DEFAULT_STREAM_CONF);
        when(rangeService.createStream(createReq)).thenReturn(
            FutureUtils.exception(CAUSE));
        AtomicReference<CreateStreamResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<CreateStreamResponse> streamObserver = new StreamObserver<CreateStreamResponse>() {
            @Override
            public void onNext(CreateStreamResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.createStream(
            CreateStreamRequest.newBuilder()
                .setNsName(nsName)
                .setName(streamName)
                .setStreamConf(DEFAULT_STREAM_CONF)
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertEquals(createResp, resultHolder.get());
        verify(rangeService, times(1)).createStream(createReq);
    }

    @Test
    public void testDeleteStreamSuccess() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        DeleteStreamResponse deleteResp = DeleteStreamResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .build();
        DeleteStreamRequest deleteReq = createDeleteStreamRequest(nsName, streamName);
        when(rangeService.deleteStream(deleteReq)).thenReturn(
            CompletableFuture.completedFuture(deleteResp));
        AtomicReference<DeleteStreamResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<DeleteStreamResponse> streamObserver = new StreamObserver<DeleteStreamResponse>() {
            @Override
            public void onNext(DeleteStreamResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.deleteStream(
            DeleteStreamRequest.newBuilder()
                .setNsName(nsName)
                .setName(streamName)
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertTrue(deleteResp == resultHolder.get());
        verify(rangeService, times(1)).deleteStream(deleteReq);
    }

    @Test
    public void testDeleteStreamFailure() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        DeleteStreamResponse deleteResp = DeleteStreamResponse.newBuilder()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
            .build();
        DeleteStreamRequest deleteReq = createDeleteStreamRequest(nsName, streamName);
        when(rangeService.deleteStream(deleteReq)).thenReturn(
            FutureUtils.exception(CAUSE));
        AtomicReference<DeleteStreamResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<DeleteStreamResponse> streamObserver = new StreamObserver<DeleteStreamResponse>() {
            @Override
            public void onNext(DeleteStreamResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.deleteStream(
            DeleteStreamRequest.newBuilder()
                .setNsName(nsName)
                .setName(streamName)
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertEquals(deleteResp, resultHolder.get());
        verify(rangeService, times(1)).deleteStream(deleteReq);
    }

    @Test
    public void testGetStreamSuccess() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        GetStreamResponse getResp = GetStreamResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setStreamProps(streamProps)
            .build();
        GetStreamRequest getReq = createGetStreamRequest(nsName, streamName);
        when(rangeService.getStream(getReq)).thenReturn(
            CompletableFuture.completedFuture(getResp));
        AtomicReference<GetStreamResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<GetStreamResponse> streamObserver = new StreamObserver<GetStreamResponse>() {
            @Override
            public void onNext(GetStreamResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.getStream(
            GetStreamRequest.newBuilder()
                .setStreamName(StreamName.newBuilder()
                    .setNamespaceName(nsName)
                    .setStreamName(streamName))
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertTrue(getResp == resultHolder.get());
        verify(rangeService, times(1)).getStream(getReq);
    }

    @Test
    public void testGetStreamFailure() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        GetStreamResponse getResp = GetStreamResponse.newBuilder()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
            .build();
        GetStreamRequest getReq = createGetStreamRequest(nsName, streamName);
        when(rangeService.getStream(getReq)).thenReturn(
            FutureUtils.exception(CAUSE));
        AtomicReference<GetStreamResponse> resultHolder = new AtomicReference<>();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<GetStreamResponse> streamObserver = new StreamObserver<GetStreamResponse>() {
            @Override
            public void onNext(GetStreamResponse resp) {
                resultHolder.set(resp);
            }

            @Override
            public void onError(Throwable t) {
                exceptionHolder.set(t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };
        grpcService.getStream(
            GetStreamRequest.newBuilder()
                .setStreamName(StreamName.newBuilder()
                    .setNamespaceName(nsName)
                    .setStreamName(streamName))
                .build(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertEquals(getResp, resultHolder.get());
        verify(rangeService, times(1)).getStream(getReq);
    }

}
