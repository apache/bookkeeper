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
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.ResponseHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.TableServiceImplBase;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.storage.api.metadata.RangeStoreService;
import org.apache.bookkeeper.stream.storage.impl.grpc.handler.ResponseHandler;

/**
 * The gRPC protocol based k/v service.
 */
@Slf4j
public class GrpcTableService extends TableServiceImplBase {

    private final RangeStoreService rangeStore;

    public GrpcTableService(RangeStoreService store) {
        this.rangeStore = store;
        log.info("Created Table service");
    }

    @Override
    public void range(RangeRequest request,
                      StreamObserver<RangeResponse> responseObserver) {
        rangeStore.range(request).whenComplete(
            new ResponseHandler<RangeResponse>(responseObserver) {
                @Override
                protected RangeResponse createErrorResp(Throwable cause) {
                    return RangeResponse.newBuilder()
                        .setHeader(ResponseHeader.newBuilder()
                            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                            .setRoutingHeader(request.getHeader())
                            .build())
                        .build();
                }
            });
    }

    @Override
    public void put(PutRequest request,
                    StreamObserver<PutResponse> responseObserver) {
        rangeStore.put(request).whenComplete(
            new ResponseHandler<PutResponse>(responseObserver) {
                @Override
                protected PutResponse createErrorResp(Throwable cause) {
                    return PutResponse.newBuilder()
                        .setHeader(ResponseHeader.newBuilder()
                            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                            .setRoutingHeader(request.getHeader())
                            .build())
                        .build();
                }
            });
    }

    @Override
    public void delete(DeleteRangeRequest request,
                       StreamObserver<DeleteRangeResponse> responseObserver) {
        rangeStore.delete(request).whenComplete(
            new ResponseHandler<DeleteRangeResponse>(responseObserver) {
                @Override
                protected DeleteRangeResponse createErrorResp(Throwable cause) {
                    return DeleteRangeResponse.newBuilder()
                        .setHeader(ResponseHeader.newBuilder()
                            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                            .setRoutingHeader(request.getHeader())
                            .build())
                        .build();
                }
            });
    }

    @Override
    public void txn(TxnRequest request,
                    StreamObserver<TxnResponse> responseObserver) {
        rangeStore.txn(request).whenComplete(
            new ResponseHandler<TxnResponse>(responseObserver) {
                @Override
                protected TxnResponse createErrorResp(Throwable cause) {
                    return TxnResponse.newBuilder()
                        .setHeader(ResponseHeader.newBuilder()
                            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                            .setRoutingHeader(request.getHeader())
                            .build())
                        .build();
                }
            });
    }

    @Override
    public void increment(IncrementRequest request,
                          StreamObserver<IncrementResponse> responseObserver) {
        rangeStore.incr(request).whenComplete(
            new ResponseHandler<IncrementResponse>(responseObserver) {
                @Override
                protected IncrementResponse createErrorResp(Throwable cause) {
                    return IncrementResponse.newBuilder()
                        .setHeader(ResponseHeader.newBuilder()
                            .setCode(StatusCode.INTERNAL_SERVER_ERROR)
                            .setRoutingHeader(request.getHeader())
                            .build())
                        .build();
                }
            });
    }
}
