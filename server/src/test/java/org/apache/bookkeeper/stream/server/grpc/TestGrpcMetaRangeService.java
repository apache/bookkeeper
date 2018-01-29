/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.server.grpc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesRequest;
import org.apache.bookkeeper.stream.proto.storage.GetActiveRangesResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerResponse;
import org.apache.bookkeeper.stream.server.TestResponseObserver;
import org.apache.bookkeeper.stream.storage.impl.RangeStoreImpl;
import org.junit.Test;

/**
 * Unit test for {@link TestGrpcMetaRangeService}.
 */
public class TestGrpcMetaRangeService {

    private static final Throwable CAUSE = new Exception("test-exception");

    //
    // Meta KeyRange Server Requests tests
    //

    @Test
    public void testGetActiveRangesSuccess() throws Exception {
        RangeStoreImpl rangeService = mock(RangeStoreImpl.class);
        GrpcMetaRangeService grpcService = new GrpcMetaRangeService(rangeService);

        StorageContainerRequest request = StorageContainerRequest
            .newBuilder()
            .setType(StorageContainerRequest.Type.GET_ACTIVE_RANGES)
            .setGetActiveRangesReq(GetActiveRangesRequest
                .newBuilder()
                .setStreamId(23456L)
                .build())
            .build();

        StorageContainerResponse response = StorageContainerResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setGetActiveRangesResp(GetActiveRangesResponse.newBuilder())
            .build();

        when(rangeService.getActiveRanges(request)).thenReturn(
            CompletableFuture.completedFuture(response));

        TestResponseObserver<StorageContainerResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.getActiveRanges(
            request,
            responseObserver);

        responseObserver.verifySuccess(response);
        verify(rangeService, times(1)).getActiveRanges(request);
    }

    @Test
    public void testGetActiveRangesFailure() throws Exception {
        RangeStoreImpl rangeService = mock(RangeStoreImpl.class);
        GrpcMetaRangeService grpcService = new GrpcMetaRangeService(rangeService);

        StorageContainerRequest request = StorageContainerRequest
            .newBuilder()
            .setType(StorageContainerRequest.Type.GET_ACTIVE_RANGES)
            .setGetActiveRangesReq(GetActiveRangesRequest
                .newBuilder()
                .setStreamId(23456L)
                .build())
            .build();

        StorageContainerResponse response = StorageContainerResponse.newBuilder()
            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
            .build();

        when(rangeService.getActiveRanges(request)).thenReturn(
            FutureUtils.exception(CAUSE));

        TestResponseObserver<StorageContainerResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.getActiveRanges(
            request,
            responseObserver);

        responseObserver.verifySuccess(response);
        verify(rangeService, times(1)).getActiveRanges(request);
    }

    @Test
    public void testGetActiveRangesException() throws Exception {
        RangeStoreImpl rangeService = mock(RangeStoreImpl.class);
        GrpcMetaRangeService grpcService = new GrpcMetaRangeService(rangeService);

        StorageContainerRequest request = StorageContainerRequest
            .newBuilder()
            .setType(StorageContainerRequest.Type.GET_ACTIVE_RANGES)
            .setGetActiveRangesReq(GetActiveRangesRequest
                .newBuilder()
                .setStreamId(23456L)
                .build())
            .build();

        when(rangeService.getActiveRanges(request)).thenReturn(
            FutureUtils.exception(new StatusRuntimeException(Status.NOT_FOUND)));

        TestResponseObserver<StorageContainerResponse> responseObserver =
            new TestResponseObserver<>();
        grpcService.getActiveRanges(
            request,
            responseObserver);

        responseObserver.verifyException(Status.NOT_FOUND);
        verify(rangeService, times(1)).getActiveRanges(request);
    }

}
