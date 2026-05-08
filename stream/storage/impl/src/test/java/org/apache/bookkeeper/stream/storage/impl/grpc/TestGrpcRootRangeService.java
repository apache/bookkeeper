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
    private static final NamespaceConfiguration namespaceConf = createNamespaceConf();

    private static NamespaceConfiguration createNamespaceConf() {
        NamespaceConfiguration conf = new NamespaceConfiguration();
        conf.setDefaultStreamConf().copyFrom(DEFAULT_STREAM_CONF);
        return conf;
    }

    private static final NamespaceProperties namespaceProps = createNamespaceProps();

    private static NamespaceProperties createNamespaceProps() {
        NamespaceProperties props = new NamespaceProperties()
            .setNamespaceId(colId)
            .setNamespaceName(nsName);
        props.setDefaultStreamConf().copyFrom(namespaceConf.getDefaultStreamConf());
        return props;
    }

    private static final String streamName = "test-stream-name";
    private static final StreamProperties streamProps = createStreamProps();

    private static StreamProperties createStreamProps() {
        StreamProperties props = new StreamProperties()
            .setStorageContainerId(1234L)
            .setStreamName(streamName)
            .setStreamId(1234L);
        props.setStreamConf().copyFrom(DEFAULT_STREAM_CONF);
        return props;
    }

    private static final Throwable CAUSE = new StorageException("test-grpc-root-range-service");

    private static CreateNamespaceRequest createNsRequest() {
        CreateNamespaceRequest req = new CreateNamespaceRequest()
            .setName(nsName);
        req.setNsConf().copyFrom(namespaceConf);
        return req;
    }

    private static DeleteNamespaceRequest deleteNsRequest() {
        return new DeleteNamespaceRequest()
            .setName(nsName);
    }

    private static GetNamespaceRequest getNsRequest() {
        return new GetNamespaceRequest()
            .setName(nsName);
    }

    private static CreateStreamRequest createStreamRequest() {
        CreateStreamRequest req = new CreateStreamRequest()
            .setNsName(nsName)
            .setName(streamName);
        req.setStreamConf().copyFrom(DEFAULT_STREAM_CONF);
        return req;
    }

    private static DeleteStreamRequest deleteStreamRequest() {
        return new DeleteStreamRequest()
            .setNsName(nsName)
            .setName(streamName);
    }

    private static GetStreamRequest getStreamRequest() {
        GetStreamRequest req = new GetStreamRequest();
        StreamName sn = req.setStreamName();
        sn.setNamespaceName(nsName);
        sn.setStreamName(streamName);
        return req;
    }

    //
    // Test Namespace API
    //

    @Test
    public void testCreateNamespaceSuccess() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcRootRangeService grpcService = new GrpcRootRangeService(rangeService);
        CreateNamespaceResponse createResp = new CreateNamespaceResponse()
            .setCode(StatusCode.SUCCESS);
        createResp.setNsProps().copyFrom(namespaceProps);
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
            createNsRequest(),
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
        CreateNamespaceResponse createResp = new CreateNamespaceResponse()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR);
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
            createNsRequest(),
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
        DeleteNamespaceResponse deleteResp = new DeleteNamespaceResponse()
            .setCode(StatusCode.SUCCESS);
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
            deleteNsRequest(),
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
        DeleteNamespaceResponse deleteResp = new DeleteNamespaceResponse()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR);
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
            deleteNsRequest(),
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
        GetNamespaceResponse getResp = new GetNamespaceResponse()
            .setCode(StatusCode.SUCCESS);
        getResp.setNsProps().copyFrom(namespaceProps);
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
            getNsRequest(),
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
        GetNamespaceResponse getResp = new GetNamespaceResponse()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR);
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
            getNsRequest(),
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
        CreateStreamResponse createResp = new CreateStreamResponse()
            .setCode(StatusCode.SUCCESS);
        createResp.setStreamProps().copyFrom(streamProps);
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
            createStreamRequest(),
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
        CreateStreamResponse createResp = new CreateStreamResponse()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR);
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
            createStreamRequest(),
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
        DeleteStreamResponse deleteResp = new DeleteStreamResponse()
            .setCode(StatusCode.SUCCESS);
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
            deleteStreamRequest(),
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
        DeleteStreamResponse deleteResp = new DeleteStreamResponse()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR);
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
            deleteStreamRequest(),
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
        GetStreamResponse getResp = new GetStreamResponse()
            .setCode(StatusCode.SUCCESS);
        getResp.setStreamProps().copyFrom(streamProps);
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
            getStreamRequest(),
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
        GetStreamResponse getResp = new GetStreamResponse()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR);
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
            getStreamRequest(),
            streamObserver);
        latch.await();
        assertNull(exceptionHolder.get());
        assertNotNull(resultHolder.get());
        assertEquals(getResp, resultHolder.get());
        verify(rangeService, times(1)).getStream(getReq);
    }

}
