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

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stream.proto.storage.MetaRangeServiceGrpc.MetaRangeServiceImplBase;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerResponse;
import org.apache.bookkeeper.stream.storage.impl.grpc.handler.StorageContainerResponseHandler;
import org.apache.bookkeeper.stream.storage.api.RangeStore;
import org.apache.bookkeeper.stream.storage.api.metadata.RangeStoreService;

/**
 * The gRPC protocol based range service.
 */
@Slf4j
public class GrpcMetaRangeService extends MetaRangeServiceImplBase {

    private final RangeStoreService rangeStore;

    public GrpcMetaRangeService(RangeStore service) {
        this.rangeStore = service;
        log.info("Created MetaRange service");
    }

    //
    // Meta KeyRange Server Requests
    //

    @Override
    public void getActiveRanges(StorageContainerRequest request,
                                StreamObserver<StorageContainerResponse> responseObserver) {
        rangeStore.getActiveRanges(request).whenComplete(
            StorageContainerResponseHandler.of(responseObserver));
    }

}
