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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.charset.StandardCharsets;
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

    private static final byte[] TEST_ROUTING_KEY = "test-routing-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TEST_KEY = "test-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TEST_VAL = "test-val".getBytes(StandardCharsets.UTF_8);

    private static RoutingHeader newRoutingHeader() {
        return new RoutingHeader()
            .setStreamId(1234L)
            .setRangeId(1234L)
            .setRKey(TEST_ROUTING_KEY);
    }

    private static PutRequest newPutRequest() {
        PutRequest req = new PutRequest()
            .setKey(TEST_KEY)
            .setValue(TEST_VAL);
        req.setHeader().copyFrom(newRoutingHeader());
        return req;
    }

    private static PutResponse newPutResponse(StatusCode code) {
        PutResponse resp = new PutResponse();
        ResponseHeader header = resp.setHeader();
        header.setCode(code);
        header.setRoutingHeader().copyFrom(newRoutingHeader());
        return resp;
    }

    private static RangeRequest newRangeRequest() {
        RangeRequest req = new RangeRequest()
            .setKey(TEST_KEY);
        req.setHeader().copyFrom(newRoutingHeader());
        return req;
    }

    private static RangeResponse newRangeResponse(StatusCode code) {
        RangeResponse resp = new RangeResponse();
        ResponseHeader header = resp.setHeader();
        header.setCode(code);
        header.setRoutingHeader().copyFrom(newRoutingHeader());
        return resp;
    }

    private static DeleteRangeRequest newDeleteRangeRequest() {
        DeleteRangeRequest req = new DeleteRangeRequest()
            .setKey(TEST_KEY);
        req.setHeader().copyFrom(newRoutingHeader());
        return req;
    }

    private static DeleteRangeResponse newDeleteRangeResponse(StatusCode code) {
        DeleteRangeResponse resp = new DeleteRangeResponse();
        ResponseHeader header = resp.setHeader();
        header.setCode(code);
        header.setRoutingHeader().copyFrom(newRoutingHeader());
        return resp;
    }

    //
    // Meta KeyRange Server Requests tests
    //

    @Test
    public void testPutSuccess() throws Exception {
        RangeStoreService rangeService = mock(RangeStoreService.class);
        GrpcTableService grpcService = new GrpcTableService(rangeService);

        PutRequest request = newPutRequest();

        PutResponse response = newPutResponse(StatusCode.SUCCESS);

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

        PutRequest request = newPutRequest();

        PutResponse response = newPutResponse(StatusCode.INTERNAL_SERVER_ERROR);

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

        PutRequest request = newPutRequest();

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

        RangeRequest request = newRangeRequest();

        RangeResponse response = newRangeResponse(StatusCode.SUCCESS);

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

        RangeRequest request = newRangeRequest();

        RangeResponse response = newRangeResponse(StatusCode.INTERNAL_SERVER_ERROR);

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

        RangeRequest request = newRangeRequest();

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

        DeleteRangeRequest request = newDeleteRangeRequest();

        DeleteRangeResponse response = newDeleteRangeResponse(StatusCode.SUCCESS);

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

        DeleteRangeRequest request = newDeleteRangeRequest();

        DeleteRangeResponse response = newDeleteRangeResponse(StatusCode.INTERNAL_SERVER_ERROR);

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

        DeleteRangeRequest request = newDeleteRangeRequest();

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
