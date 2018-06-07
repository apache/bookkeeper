/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.stream.server.grpc;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stream.proto.common.Endpoint;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointRequest;
import org.apache.bookkeeper.stream.proto.storage.GetStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.OneStorageContainerEndpointResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerEndpoint;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerServiceGrpc.StorageContainerServiceImplBase;
import org.apache.bookkeeper.stream.storage.api.StorageContainerStore;

/**
 * Grpc based storage container service.
 */
@Slf4j
class GrpcStorageContainerService extends StorageContainerServiceImplBase {

    private final StorageContainerStore storageContainerStore;

    GrpcStorageContainerService(StorageContainerStore storageContainerStore) {
        this.storageContainerStore = storageContainerStore;
    }

    @Override
    public void getStorageContainerEndpoint(GetStorageContainerEndpointRequest request,
                                            StreamObserver<GetStorageContainerEndpointResponse> responseObserver) {
        GetStorageContainerEndpointResponse.Builder responseBuilder = GetStorageContainerEndpointResponse.newBuilder()
            .setStatusCode(StatusCode.SUCCESS);
        for (int i = 0; i < request.getRequestsCount(); i++) {
            Endpoint endpoint = storageContainerStore
                .getRoutingService()
                .getStorageContainer(request.getRequests(i).getStorageContainer());
            OneStorageContainerEndpointResponse.Builder oneRespBuilder;
            if (null != endpoint) {
                oneRespBuilder = OneStorageContainerEndpointResponse.newBuilder()
                    .setStatusCode(StatusCode.SUCCESS)
                    .setEndpoint(
                        StorageContainerEndpoint.newBuilder()
                            .setRwEndpoint(endpoint)
                            .addRoEndpoint(endpoint)
                            .setRevision(0L));
            } else {
                oneRespBuilder = OneStorageContainerEndpointResponse.newBuilder()
                    .setStatusCode(StatusCode.INTERNAL_SERVER_ERROR);
            }
            responseBuilder = responseBuilder.addResponses(oneRespBuilder);
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

}
