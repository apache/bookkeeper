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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.ResponseHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.storage.api.metadata.RangeStoreService;
import org.junit.Test;

/**
 * Unit test for {@link TestGrpcTableService}.
 */
public class TestGrpcTableService {

    private static final Throwable CAUSE = new Exception("test-exception");

    private static final ByteString TEST_ROUTING_KEY = ByteString.copyFromUtf8("test-routing-key");
    private static final RoutingHeader ROUTING_HEADER = RoutingHeader.newBuilder()
        .setStreamId(1234L)
        .setRangeId(1234L)
        .setRKey(TEST_ROUTING_KEY)
        .build();
    private static final ByteString TEST_KEY = ByteString.copyFromUtf8("test-key");
    private static final ByteString TEST_VAL = ByteString.copyFromUtf8("test-val");

    //
    // Meta KeyRange Server Requests tests
    //

    @Test
    public void testPutSuccess() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcTableService grpcService = new GrpcTableService(rangeService);

        PutRequest request = PutRequest
            .newBuilder()
            .setKey(TEST_KEY)
            .setValue(TEST_VAL)
            .setHeader(ROUTING_HEADER)
            .build();

        PutResponse response = PutResponse.newBuilder()
            .setHeader(ResponseHeader.newBuilder()
                .setCode(StatusCode.SUCCESS)
                .setRoutingHeader(ROUTING_HEADER)
                .build())
            .build();

        when(rangeService.put(request)).thenReturn(
            CompletableFuture.completedFuture(response));

        TestResponseObserver<PutResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.put(
            request,
            responseObserver);

        responseObserver.verifySuccess(response);
        verify(rangeService, times(1)).put(request);
    }

    @Test
    public void testPutFailure() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcTableService grpcService = new GrpcTableService(rangeService);

        PutRequest request = PutRequest
            .newBuilder()
            .setKey(TEST_KEY)
            .setValue(TEST_VAL)
            .setHeader(ROUTING_HEADER)
            .build();

        PutResponse response = PutResponse.newBuilder()
            .setHeader(ResponseHeader.newBuilder()
                .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                .setRoutingHeader(ROUTING_HEADER)
                .build())
            .build();

        when(rangeService.put(request)).thenReturn(
            FutureUtils.exception(CAUSE));

        TestResponseObserver<PutResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.put(
            request,
            responseObserver);

        responseObserver.verifySuccess(response);
        verify(rangeService, times(1)).put(request);
    }

    @Test
    public void testPutException() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcTableService grpcService = new GrpcTableService(rangeService);

        PutRequest request = PutRequest
            .newBuilder()
            .setKey(TEST_KEY)
            .setValue(TEST_VAL)
            .setHeader(ROUTING_HEADER)
            .build();

        when(rangeService.put(request)).thenReturn(
            FutureUtils.exception(new StatusRuntimeException(Status.NOT_FOUND)));

        TestResponseObserver<PutResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.put(
            request,
            responseObserver);

        responseObserver.verifyException(Status.NOT_FOUND);
        verify(rangeService, times(1)).put(request);
    }

    @Test
    public void testRangeSuccess() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcTableService grpcService = new GrpcTableService(rangeService);

        RangeRequest request = RangeRequest
            .newBuilder()
            .setKey(TEST_KEY)
            .setHeader(ROUTING_HEADER)
            .build();

        RangeResponse response = RangeResponse.newBuilder()
            .setHeader(ResponseHeader.newBuilder()
                .setCode(StatusCode.SUCCESS)
                .setRoutingHeader(ROUTING_HEADER)
                .build())
            .build();

        when(rangeService.range(request)).thenReturn(
            CompletableFuture.completedFuture(response));

        TestResponseObserver<RangeResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.range(
            request,
            responseObserver);

        responseObserver.verifySuccess(response);
        verify(rangeService, times(1)).range(request);
    }

    @Test
    public void testRangeActiveRangesFailure() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcTableService grpcService = new GrpcTableService(rangeService);

        RangeRequest request = RangeRequest
            .newBuilder()
            .setKey(TEST_KEY)
            .setHeader(ROUTING_HEADER)
            .build();

        RangeResponse response = RangeResponse.newBuilder()
            .setHeader(ResponseHeader.newBuilder()
                .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                .setRoutingHeader(ROUTING_HEADER)
                .build())
            .build();

        when(rangeService.range(request)).thenReturn(
            FutureUtils.exception(CAUSE));

        TestResponseObserver<RangeResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.range(
            request,
            responseObserver);

        responseObserver.verifySuccess(response);
        verify(rangeService, times(1)).range(request);
    }

    @Test
    public void testRangeActiveRangesException() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcTableService grpcService = new GrpcTableService(rangeService);

        RangeRequest request = RangeRequest
            .newBuilder()
            .setKey(TEST_KEY)
            .setHeader(ROUTING_HEADER)
            .build();

        when(rangeService.range(request)).thenReturn(
            FutureUtils.exception(new StatusRuntimeException(Status.NOT_FOUND)));

        TestResponseObserver<RangeResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.range(
            request,
            responseObserver);

        responseObserver.verifyException(Status.NOT_FOUND);
        verify(rangeService, times(1)).range(request);
    }

    @Test
    public void testDeleteSuccess() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcTableService grpcService = new GrpcTableService(rangeService);

        DeleteRangeRequest request = DeleteRangeRequest
            .newBuilder()
            .setKey(TEST_KEY)
            .setHeader(ROUTING_HEADER)
            .build();

        DeleteRangeResponse response = DeleteRangeResponse.newBuilder()
            .setHeader(ResponseHeader.newBuilder()
                .setCode(StatusCode.SUCCESS)
                .setRoutingHeader(ROUTING_HEADER)
                .build())
            .build();

        when(rangeService.delete(request)).thenReturn(
            CompletableFuture.completedFuture(response));

        TestResponseObserver<DeleteRangeResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.delete(
            request,
            responseObserver);

        responseObserver.verifySuccess(response);
        verify(rangeService, times(1)).delete(request);
    }

    @Test
    public void testDeleteFailure() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcTableService grpcService = new GrpcTableService(rangeService);

        DeleteRangeRequest request = DeleteRangeRequest
            .newBuilder()
            .setKey(TEST_KEY)
            .setHeader(ROUTING_HEADER)
            .build();

        DeleteRangeResponse response = DeleteRangeResponse.newBuilder()
            .setHeader(ResponseHeader.newBuilder()
                .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                .setRoutingHeader(ROUTING_HEADER)
                .build())
            .build();

        when(rangeService.delete(request)).thenReturn(
            FutureUtils.exception(CAUSE));

        TestResponseObserver<DeleteRangeResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.delete(
            request,
            responseObserver);

        responseObserver.verifySuccess(response);
        verify(rangeService, times(1)).delete(request);
    }

    @Test
    public void testDeleteException() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcTableService grpcService = new GrpcTableService(rangeService);

        DeleteRangeRequest request = DeleteRangeRequest
            .newBuilder()
            .setKey(TEST_KEY)
            .setHeader(ROUTING_HEADER)
            .build();

        when(rangeService.delete(request)).thenReturn(
            FutureUtils.exception(new StatusRuntimeException(Status.NOT_FOUND)));

        TestResponseObserver<DeleteRangeResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.delete(
            request,
            responseObserver);

        responseObserver.verifyException(Status.NOT_FOUND);
        verify(rangeService, times(1)).delete(request);
    }

}
