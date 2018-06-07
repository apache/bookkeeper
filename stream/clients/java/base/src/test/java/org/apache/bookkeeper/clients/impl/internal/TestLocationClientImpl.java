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

package org.apache.bookkeeper.clients.impl.internal;

import static org.apache.bookkeeper.clients.impl.internal.LocationClientImpl.LOCATE_STORAGE_CONTAINERS_RETRY_PREDICATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.exceptions.StorageContainerException;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.utils.NetUtils;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.IRevisioned;
import org.apache.bookkeeper.common.util.Revisioned;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointRequest;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerEndpoint;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerServiceGrpc.StorageContainerServiceImplBase;
import org.junit.Test;

/**
 * Unit test for {@link LocationClientImpl}.
 */
@Slf4j
public class TestLocationClientImpl extends GrpcClientTestBase {

    private static StorageContainerEndpoint createEndpoint(int groupId) {
        return StorageContainerEndpoint.newBuilder()
            .setStorageContainerId(groupId)
            .setRevision(1000L + groupId)
            .setRwEndpoint(NetUtils.createEndpoint("127.0.0." + groupId, groupId))
            .addRoEndpoint(NetUtils.createEndpoint("128.0.0." + groupId, groupId))
            .build();
    }

    private LocationClientImpl locationClient;

    private final List<StorageContainerEndpoint> endpoints = IntStream
        .range(0, 10)
        .boxed()
        .map(i -> createEndpoint(i))
        .collect(Collectors.toList());

    private final StorageContainerServiceImplBase locationService = new StorageContainerServiceImplBase() {

        @Override
        public void getStorageContainerEndpoint(GetStorageContainerEndpointRequest request,
                                                StreamObserver<GetStorageContainerEndpointResponse> responseObserver) {
            GetStorageContainerEndpointResponse.Builder respBuilder = GetStorageContainerEndpointResponse.newBuilder();
            if (0 == request.getRequestsCount()) {
                responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
            } else {
                for (OneStorageContainerEndpointRequest oneRequest : request.getRequestsList()) {
                    respBuilder.addResponses(processOneStorageContainerEndpointRequest(oneRequest));
                }
                respBuilder.setStatusCode(StatusCode.SUCCESS);
                responseObserver.onNext(respBuilder.build());
            }
            responseObserver.onCompleted();
        }

        OneStorageContainerEndpointResponse.Builder processOneStorageContainerEndpointRequest(
            OneStorageContainerEndpointRequest request) {
            StatusCode code;
            StorageContainerEndpoint endpoint = null;
            if (request.getStorageContainer() < 0) {
                code = StatusCode.INVALID_GROUP_ID;
            } else if (request.getStorageContainer() >= endpoints.size()) {
                code = StatusCode.GROUP_NOT_FOUND;
            } else {
                code = StatusCode.SUCCESS;
                endpoint = endpoints.get((int) request.getStorageContainer());
            }
            if (endpoint != null) {
                if (endpoint.getRevision() <= request.getRevision()) {
                    code = StatusCode.STALE_GROUP_INFO;
                    endpoint = null;
                }
            }
            OneStorageContainerEndpointResponse.Builder builder = OneStorageContainerEndpointResponse.newBuilder()
                .setStatusCode(code);
            if (null != endpoint) {
                builder = builder.setEndpoint(endpoint);
            }
            return builder;
        }
    };
    private ServerServiceDefinition locationServiceDefinition;

    @Override
    protected void doSetup() throws Exception {
        StorageClientSettings settings =
            StorageClientSettings.newBuilder()
                .serviceUri("bk+inprocess://" + serverName)
                .build();
        locationClient = new LocationClientImpl(settings, scheduler);
        locationServiceDefinition = locationService.bindService();
        serviceRegistry.addService(locationServiceDefinition);
    }

    @Override
    protected void doTeardown() throws Exception {
        if (null != locationClient) {
            locationClient.close();
        }
    }

    private void assertOneStorageContainerEndpointResponse(
        OneStorageContainerEndpointResponse response,
        StatusCode expectedStatusCode,
        StorageContainerEndpoint expectedEndpoint) {
        assertEquals(expectedStatusCode, response.getStatusCode());
        if (null != expectedEndpoint) {
            assertEquals("Expected endpoint = " + expectedEndpoint + ", Actual endpoint = " + response.getEndpoint(),
                expectedEndpoint, response.getEndpoint());
        } else {
            assertFalse(response.hasEndpoint());
        }
    }

    @Test
    public void testLocateStorageContainersSuccess() throws Exception {
        CompletableFuture<List<OneStorageContainerEndpointResponse>> future =
            locationClient.locateStorageContainers(Lists.newArrayList(
                Revisioned.of(1L, IRevisioned.ANY_REVISION),
                Revisioned.of(3L, IRevisioned.ANY_REVISION),
                Revisioned.of(5L, IRevisioned.ANY_REVISION),
                Revisioned.of(7L, IRevisioned.ANY_REVISION)
            ));
        List<OneStorageContainerEndpointResponse> result = FutureUtils.result(future);
        assertEquals(4, result.size());
        assertOneStorageContainerEndpointResponse(result.get(0), StatusCode.SUCCESS, endpoints.get(1));
        assertOneStorageContainerEndpointResponse(result.get(1), StatusCode.SUCCESS, endpoints.get(3));
        assertOneStorageContainerEndpointResponse(result.get(2), StatusCode.SUCCESS, endpoints.get(5));
        assertOneStorageContainerEndpointResponse(result.get(3), StatusCode.SUCCESS, endpoints.get(7));
    }

    @Test
    public void testLocateStorageContainersInvalidArgs() throws Exception {
        CompletableFuture<List<OneStorageContainerEndpointResponse>> future =
            locationClient.locateStorageContainers(Lists.newArrayList());
        try {
            future.get();
            fail("Should fail with invalid arguments");
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            assertTrue(
                "Unexpected exception : " + cause,
                cause instanceof StatusRuntimeException);
            assertEquals(Status.INVALID_ARGUMENT, ((StatusRuntimeException) cause).getStatus());
        }
    }

    @Test
    public void testLocateStorageContainersFailures() throws Exception {
        CompletableFuture<List<OneStorageContainerEndpointResponse>> future =
            locationClient.locateStorageContainers(Lists.newArrayList(
                Revisioned.of(-1L, IRevisioned.ANY_REVISION), // invalid group id
                Revisioned.of(1L, IRevisioned.ANY_REVISION), // valid group id
                Revisioned.of(3L, Long.MAX_VALUE), // stale revision
                Revisioned.of(Long.MAX_VALUE, IRevisioned.ANY_REVISION) // not found
            ));
        List<OneStorageContainerEndpointResponse> result = FutureUtils.result(future);
        assertEquals(4, result.size());
        assertOneStorageContainerEndpointResponse(result.get(0), StatusCode.INVALID_GROUP_ID, null);
        assertOneStorageContainerEndpointResponse(result.get(1), StatusCode.SUCCESS, endpoints.get(1));
        assertOneStorageContainerEndpointResponse(result.get(2), StatusCode.STALE_GROUP_INFO, null);
        assertOneStorageContainerEndpointResponse(result.get(3), StatusCode.GROUP_NOT_FOUND, null);
    }

    @Test
    public void testLocateStorageContainersRetryPredicate() throws Exception {
        assertTrue(LOCATE_STORAGE_CONTAINERS_RETRY_PREDICATE.test(
            new StatusException(Status.INTERNAL)));
        assertTrue(LOCATE_STORAGE_CONTAINERS_RETRY_PREDICATE.test(
            new StatusRuntimeException(Status.INTERNAL)));
        assertFalse(LOCATE_STORAGE_CONTAINERS_RETRY_PREDICATE.test(
            new StatusException(Status.INVALID_ARGUMENT)));
        assertFalse(LOCATE_STORAGE_CONTAINERS_RETRY_PREDICATE.test(
            new StatusRuntimeException(Status.INVALID_ARGUMENT)));
        assertTrue(LOCATE_STORAGE_CONTAINERS_RETRY_PREDICATE.test(
            new ClientException("test-2")));
        assertTrue(LOCATE_STORAGE_CONTAINERS_RETRY_PREDICATE.test(
            new StorageContainerException(StatusCode.FAILURE, "test-3")));
    }

    @Test
    public void testLocateStorageContainersSucceedAfterRetried() throws Exception {
        serviceRegistry.removeService(locationServiceDefinition);
        final AtomicInteger retries = new AtomicInteger(3);
        StatusRuntimeException statusException = new StatusRuntimeException(Status.INTERNAL);
        StorageContainerServiceImplBase locationServiceWithFailures =
            new StorageContainerServiceImplBase() {
                @Override
                public void getStorageContainerEndpoint(
                    GetStorageContainerEndpointRequest request,
                    StreamObserver<GetStorageContainerEndpointResponse> responseObserver) {
                    if (retries.decrementAndGet() == 0) {
                        locationService.getStorageContainerEndpoint(request, responseObserver);
                        return;
                    }
                    responseObserver.onError(statusException);
                }
            };
        serviceRegistry.addService(locationServiceWithFailures.bindService());
        testLocateStorageContainersSuccess();
        assertEquals(0, retries.get());
    }

    @Test
    public void testLocateStorageContainersFailureAfterRetried() throws Exception {
        serviceRegistry.removeService(locationServiceDefinition);
        final AtomicInteger retries = new AtomicInteger(3);
        StatusRuntimeException statusException = new StatusRuntimeException(Status.INTERNAL);
        StorageContainerServiceImplBase locationServiceWithFailures =
            new StorageContainerServiceImplBase() {
                @Override
                public void getStorageContainerEndpoint(
                    GetStorageContainerEndpointRequest request,
                    StreamObserver<GetStorageContainerEndpointResponse> responseObserver) {
                    if (retries.decrementAndGet() == 0) {
                        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT));
                        return;
                    }
                    responseObserver.onError(statusException);
                }
            };
        serviceRegistry.addService(locationServiceWithFailures.bindService());
        CompletableFuture<List<OneStorageContainerEndpointResponse>> future =
            locationClient.locateStorageContainers(Lists.newArrayList(
                Revisioned.of(1L, IRevisioned.ANY_REVISION)
            ));
        try {
            future.get();
            fail("should fail with exception");
        } catch (ExecutionException ee) {
            assertNotNull(ee.getCause());
            assertTrue(ee.getCause() instanceof StatusRuntimeException);
            assertEquals(Status.INVALID_ARGUMENT, ((StatusRuntimeException) ee.getCause()).getStatus());
        }
        assertEquals(0, retries.get());
    }

}
