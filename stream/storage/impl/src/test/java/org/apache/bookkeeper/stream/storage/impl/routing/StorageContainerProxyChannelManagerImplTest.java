/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.stream.storage.impl.routing;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointRequest;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerEndpoint;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerServiceGrpc.StorageContainerServiceImplBase;
import org.junit.Test;

/**
 * Unit testing {@link StorageContainerProxyChannelManagerImpl}.
 */
public class StorageContainerProxyChannelManagerImplTest extends GrpcClientTestBase {

    private final long scId = 1234L;
    private StorageContainerProxyChannelManagerImpl proxyChannelManager;

    @Override
    protected void doSetup() throws Exception {
        this.proxyChannelManager = new StorageContainerProxyChannelManagerImpl(serverManager);
    }

    @Override
    protected void doTeardown() throws Exception {
    }


    @Test
    public void testGetStorageContainerChannel() throws Exception {
        final CompletableFuture<GetStorageContainerEndpointRequest> receivedRequest = new CompletableFuture<>();
        final CompletableFuture<GetStorageContainerEndpointResponse> responseSupplier = new CompletableFuture<>();
        StorageContainerServiceImplBase scService = new StorageContainerServiceImplBase() {
            @Override
            public void getStorageContainerEndpoint(
                    GetStorageContainerEndpointRequest request,
                    StreamObserver<GetStorageContainerEndpointResponse> responseObserver) {
                receivedRequest.complete(request);
                try {
                    responseObserver.onNext(responseSupplier.get());
                    responseObserver.onCompleted();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    responseObserver.onError(e);
                } catch (ExecutionException e) {
                    responseObserver.onError(e);
                }
            }
        };
        serviceRegistry.addService(scService.bindService());

        Channel channel = proxyChannelManager.getStorageContainerChannel(scId);
        // if the location service doesn't respond, the channel will be null
        assertNull(channel);

        // complete the location service request
        responseSupplier.complete(getResponse(receivedRequest.get()));
        while ((channel = proxyChannelManager.getStorageContainerChannel(scId)) == null) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        assertNotNull(channel);
    }

    private static GetStorageContainerEndpointResponse getResponse(GetStorageContainerEndpointRequest request) {
        GetStorageContainerEndpointResponse.Builder respBuilder =
            GetStorageContainerEndpointResponse.newBuilder();
        respBuilder.setStatusCode(StatusCode.SUCCESS);
        for (OneStorageContainerEndpointRequest oneReq : request.getRequestsList()) {
            OneStorageContainerEndpointResponse oneResp = OneStorageContainerEndpointResponse.newBuilder()
                .setEndpoint(StorageContainerEndpoint.newBuilder()
                    .setStorageContainerId(oneReq.getStorageContainer())
                    .setRevision(oneReq.getRevision() + 1)
                    .setRwEndpoint(ENDPOINT))
                .build();
            respBuilder.addResponses(oneResp);
        }
        return respBuilder.build();
    }



}
