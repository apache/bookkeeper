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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.RID_METADATA_KEY;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.RK_METADATA_KEY;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.SID_METADATA_KEY;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors.CheckedForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.ResponseHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.TableServiceImplBase;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.junit.Test;

/**
 * Unit test {@link RoutingHeaderProxyInterceptor}.
 */
@Slf4j
public class RoutingHeaderProxyInterceptorTest extends GrpcClientTestBase {

    private final long streamId = 1234L;
    private final long rangeId = 2345L;
    private final byte[] routingKey = ("routing-key-" + System.currentTimeMillis()).getBytes(UTF_8);
    private final AtomicReference<Object> receivedRequest = new AtomicReference<>();
    private StorageServerChannel channel;

    @Override
    protected void doSetup() {
        TableServiceImplBase tableService = new TableServiceImplBase() {

            @Override
            public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
                log.info("Received range request : {}", request);
                receivedRequest.set(request);
                responseObserver.onNext(RangeResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setCode(StatusCode.SUCCESS)
                        .setRoutingHeader(request.getHeader())
                        .build())
                    .build());
                responseObserver.onCompleted();
            }

            @Override
            public void delete(DeleteRangeRequest request, StreamObserver<DeleteRangeResponse> responseObserver) {
                log.info("Received delete range request : {}", request);
                receivedRequest.set(request);
                responseObserver.onNext(DeleteRangeResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setCode(StatusCode.SUCCESS)
                        .setRoutingHeader(request.getHeader())
                        .build())
                    .build());
                responseObserver.onCompleted();
            }

            @Override
            public void txn(TxnRequest request, StreamObserver<TxnResponse> responseObserver) {
                log.info("Received txn request : {}", request);
                receivedRequest.set(request);
                responseObserver.onNext(TxnResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setCode(StatusCode.SUCCESS)
                        .setRoutingHeader(request.getHeader())
                        .build())
                    .build());
                responseObserver.onCompleted();
            }

            @Override
            public void increment(IncrementRequest request, StreamObserver<IncrementResponse> responseObserver) {
                log.info("Received incr request : {}", request);
                receivedRequest.set(request);
                responseObserver.onNext(IncrementResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setCode(StatusCode.SUCCESS)
                        .setRoutingHeader(request.getHeader())
                        .build())
                    .build());
                responseObserver.onCompleted();
            }

            @Override
            public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
                log.info("Received put request : {}", request);
                receivedRequest.set(request);
                responseObserver.onNext(PutResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setCode(StatusCode.SUCCESS)
                        .setRoutingHeader(request.getHeader())
                        .build())
                    .build());
                responseObserver.onCompleted();
            }
        };
        serviceRegistry.addService(tableService.bindService());


        this.channel = new StorageServerChannel(
            InProcessChannelBuilder.forName(serverName).directExecutor().build(),
            Optional.empty()
        ).intercept(
            new RoutingHeaderProxyInterceptor(),
            new ClientInterceptor() {
                @Override
                public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                           CallOptions callOptions,
                                                                           Channel next) {
                    return new CheckedForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                        @Override
                        protected void checkedStart(Listener<RespT> responseListener, Metadata headers) {
                            log.info("Intercept the request with routing information : sid = {}, rid = {}, rk = {}",
                                streamId, rangeId, new String(routingKey, UTF_8));
                            headers.put(
                                RID_METADATA_KEY,
                                rangeId
                            );
                            headers.put(
                                SID_METADATA_KEY,
                                streamId
                            );
                            headers.put(
                                RK_METADATA_KEY,
                                routingKey
                            );
                            delegate().start(responseListener, headers);
                        }
                    };
                }
            }
        );
    }

    @Override
    protected void doTeardown() {
        channel.close();
    }

    @Test
    public void testPutRequest() throws Exception {
        PutRequest request = PutRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("test-key"))
            .build();
        PutRequest expectedRequest = PutRequest.newBuilder(request)
            .setHeader(RoutingHeader.newBuilder(request.getHeader())
                .setStreamId(streamId)
                .setRangeId(rangeId)
                .setRKey(ByteString.copyFrom(routingKey))
                .build())
            .build();
        PutResponse response = this.channel.getTableService().put(request).get();

        assertEquals(expectedRequest, receivedRequest.get());
        assertEquals(expectedRequest.getHeader(), response.getHeader().getRoutingHeader());
    }

    @Test
    public void testRangeRequest() throws Exception {
        RangeRequest request = RangeRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("test-key"))
            .build();
        RangeRequest expectedRequest = RangeRequest.newBuilder(request)
            .setHeader(RoutingHeader.newBuilder(request.getHeader())
                .setStreamId(streamId)
                .setRangeId(rangeId)
                .setRKey(ByteString.copyFrom(routingKey))
                .build())
            .build();
        RangeResponse response = this.channel.getTableService()
            .range(request).get();

        assertEquals(expectedRequest, receivedRequest.get());
        assertEquals(expectedRequest.getHeader(), response.getHeader().getRoutingHeader());
    }

    @Test
    public void testDeleteRangeRequest() throws Exception {
        DeleteRangeRequest request = DeleteRangeRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("test-key"))
            .build();
        DeleteRangeRequest expectedRequest = DeleteRangeRequest.newBuilder(request)
            .setHeader(RoutingHeader.newBuilder(request.getHeader())
                .setStreamId(streamId)
                .setRangeId(rangeId)
                .setRKey(ByteString.copyFrom(routingKey))
                .build())
            .build();
        DeleteRangeResponse response = this.channel.getTableService()
            .delete(request).get();

        assertEquals(expectedRequest, receivedRequest.get());
        assertEquals(expectedRequest.getHeader(), response.getHeader().getRoutingHeader());
    }

    @Test
    public void testIncrementRequest() throws Exception {
        IncrementRequest request = IncrementRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("test-key"))
            .build();
        IncrementRequest expectedRequest = IncrementRequest.newBuilder(request)
            .setHeader(RoutingHeader.newBuilder(request.getHeader())
                .setStreamId(streamId)
                .setRangeId(rangeId)
                .setRKey(ByteString.copyFrom(routingKey))
                .build())
            .build();
        IncrementResponse response = this.channel.getTableService()
            .increment(request).get();

        assertEquals(expectedRequest, receivedRequest.get());
        assertEquals(expectedRequest.getHeader(), response.getHeader().getRoutingHeader());
    }

    @Test
    public void testTxnRequest() throws Exception {
        TxnRequest request = TxnRequest.newBuilder()
            .build();
        TxnRequest expectedRequest = TxnRequest.newBuilder(request)
            .setHeader(RoutingHeader.newBuilder(request.getHeader())
                .setStreamId(streamId)
                .setRangeId(rangeId)
                .setRKey(ByteString.copyFrom(routingKey))
                .build())
            .build();
        TxnResponse response = this.channel.getTableService().txn(request).get();

        assertEquals(expectedRequest, receivedRequest.get());
        assertEquals(expectedRequest.getHeader(), response.getHeader().getRoutingHeader());
    }

}
