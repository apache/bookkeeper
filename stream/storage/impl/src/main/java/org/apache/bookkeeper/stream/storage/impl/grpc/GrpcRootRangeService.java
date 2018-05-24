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
import org.apache.bookkeeper.stream.proto.storage.RootRangeServiceGrpc.RootRangeServiceImplBase;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.storage.api.metadata.RangeStoreService;
import org.apache.bookkeeper.stream.storage.impl.grpc.handler.ResponseHandler;

/**
 * Grpc based root range service.
 */
public class GrpcRootRangeService extends RootRangeServiceImplBase {

    private final RangeStoreService rs;

    public GrpcRootRangeService(RangeStoreService rs) {
        this.rs = rs;
    }

    //
    // Namespace API
    //

    @Override
    public void createNamespace(CreateNamespaceRequest request,
                                StreamObserver<CreateNamespaceResponse> responseObserver) {
        rs.createNamespace(request).whenComplete(
            new ResponseHandler<CreateNamespaceResponse>(responseObserver) {
                @Override
                protected CreateNamespaceResponse createErrorResp(Throwable cause) {
                    return CreateNamespaceResponse.newBuilder()
                        .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                        .build();
                }
            });
    }

    @Override
    public void deleteNamespace(DeleteNamespaceRequest request,
                                StreamObserver<DeleteNamespaceResponse> responseObserver) {
        rs.deleteNamespace(request).whenComplete(
            new ResponseHandler<DeleteNamespaceResponse>(responseObserver) {
                @Override
                protected DeleteNamespaceResponse createErrorResp(Throwable cause) {
                    return DeleteNamespaceResponse.newBuilder()
                        .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                        .build();
                }
            });
    }

    @Override
    public void getNamespace(GetNamespaceRequest request,
                             StreamObserver<GetNamespaceResponse> responseObserver) {
        rs.getNamespace(request).whenComplete(
            new ResponseHandler<GetNamespaceResponse>(responseObserver) {
                @Override
                protected GetNamespaceResponse createErrorResp(Throwable cause) {
                    return GetNamespaceResponse.newBuilder()
                        .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                        .build();
                }
            });
    }

    //
    // Stream API
    //

    @Override
    public void createStream(CreateStreamRequest request,
                             StreamObserver<CreateStreamResponse> responseObserver) {
        rs.createStream(request).whenComplete(
            new ResponseHandler<CreateStreamResponse>(responseObserver) {
                @Override
                protected CreateStreamResponse createErrorResp(Throwable cause) {
                    return CreateStreamResponse.newBuilder()
                        .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                        .build();
                }
            });
    }

    @Override
    public void deleteStream(DeleteStreamRequest request,
                             StreamObserver<DeleteStreamResponse> responseObserver) {
        rs.deleteStream(request).whenComplete(
            new ResponseHandler<DeleteStreamResponse>(responseObserver) {
                @Override
                protected DeleteStreamResponse createErrorResp(Throwable cause) {
                    return DeleteStreamResponse.newBuilder()
                        .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                        .build();
                }
            });
    }

    @Override
    public void getStream(GetStreamRequest request,
                          StreamObserver<GetStreamResponse> responseObserver) {
        rs.getStream(request).whenComplete(
            new ResponseHandler<GetStreamResponse>(responseObserver) {
                @Override
                protected GetStreamResponse createErrorResp(Throwable cause) {
                    return GetStreamResponse.newBuilder()
                        .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                        .build();
                }
            });
    }

}
