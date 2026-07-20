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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors.CheckedForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.bookkeeper.clients.grpc.GrpcClientTestBase;
import org.apache.bookkeeper.clients.impl.channel.StorageServerChannel;
import org.apache.bookkeeper.common.grpc.netty.IdentityInputStreamMarshaller;
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
import org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc;
import org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.TableServiceImplBase;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.junit.Test;

/**
 * Unit test {@link RoutingHeaderProxyInterceptor}.
 */
@CustomLog
public class RoutingHeaderProxyInterceptorTest extends GrpcClientTestBase {

    private final long streamId = 1234L;
    private final long rangeId = 2345L;
    private final byte[] routingKey = ("routing-key-" + System.currentTimeMillis()).getBytes(UTF_8);
    private final AtomicReference<Object> receivedRequest = new AtomicReference<>();
    private StorageServerChannel channel;

    private static RangeResponse newRangeResponse(RoutingHeader header) {
        RangeResponse resp = new RangeResponse();
        ResponseHeader rh = resp.setHeader();
        rh.setCode(StatusCode.SUCCESS);
        rh.setRoutingHeader().copyFrom(header);
        return resp;
    }

    private static DeleteRangeResponse newDeleteRangeResponse(RoutingHeader header) {
        DeleteRangeResponse resp = new DeleteRangeResponse();
        ResponseHeader rh = resp.setHeader();
        rh.setCode(StatusCode.SUCCESS);
        rh.setRoutingHeader().copyFrom(header);
        return resp;
    }

    private static TxnResponse newTxnResponse(RoutingHeader header) {
        TxnResponse resp = new TxnResponse();
        ResponseHeader rh = resp.setHeader();
        rh.setCode(StatusCode.SUCCESS);
        rh.setRoutingHeader().copyFrom(header);
        return resp;
    }

    private static IncrementResponse newIncrementResponse(RoutingHeader header) {
        IncrementResponse resp = new IncrementResponse();
        ResponseHeader rh = resp.setHeader();
        rh.setCode(StatusCode.SUCCESS);
        rh.setRoutingHeader().copyFrom(header);
        return resp;
    }

    private static PutResponse newPutResponse(RoutingHeader header) {
        PutResponse resp = new PutResponse();
        ResponseHeader rh = resp.setHeader();
        rh.setCode(StatusCode.SUCCESS);
        rh.setRoutingHeader().copyFrom(header);
        return resp;
    }

    @Override
    protected void doSetup() {
        TableServiceImplBase tableService = new TableServiceImplBase() {

            @Override
            public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
                log.info().attr("request", request).log("Received range request");
                receivedRequest.set(request);
                responseObserver.onNext(newRangeResponse(request.getHeader()));
                responseObserver.onCompleted();
            }

            @Override
            public void delete(DeleteRangeRequest request, StreamObserver<DeleteRangeResponse> responseObserver) {
                log.info().attr("request", request).log("Received delete range request");
                receivedRequest.set(request);
                responseObserver.onNext(newDeleteRangeResponse(request.getHeader()));
                responseObserver.onCompleted();
            }

            @Override
            public void txn(TxnRequest request, StreamObserver<TxnResponse> responseObserver) {
                log.info().attr("request", request).log("Received txn request");
                receivedRequest.set(request);
                responseObserver.onNext(newTxnResponse(request.getHeader()));
                responseObserver.onCompleted();
            }

            @Override
            public void increment(IncrementRequest request, StreamObserver<IncrementResponse> responseObserver) {
                log.info().attr("request", request).log("Received incr request");
                receivedRequest.set(request);
                responseObserver.onNext(newIncrementResponse(request.getHeader()));
                responseObserver.onCompleted();
            }

            @Override
            public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
                log.info().attr("request", request).log("Received put request");
                receivedRequest.set(request);
                responseObserver.onNext(newPutResponse(request.getHeader()));
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
                            log.info()
                                .attr("streamId", streamId)
                                .attr("rangeId", rangeId)
                                .attr("routingKey", new String(routingKey, UTF_8))
                                .log("Intercept the request with routing information");
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

    private static RoutingHeader newRoutingHeader(long streamId, long rangeId, byte[] rk) {
        return new RoutingHeader()
            .setStreamId(streamId)
            .setRangeId(rangeId)
            .setRKey(rk);
    }

    @Test
    public void testPutRequest() throws Exception {
        PutRequest request = new PutRequest()
            .setKey("test-key".getBytes(UTF_8));
        PutRequest expectedRequest = new PutRequest();
        expectedRequest.copyFrom(request);
        expectedRequest.setHeader().copyFrom(newRoutingHeader(streamId, rangeId, routingKey));
        PutResponse response = this.channel.getTableService().put(request).get();

        assertEquals(expectedRequest, receivedRequest.get());
        assertEquals(expectedRequest.getHeader(), response.getHeader().getRoutingHeader());
    }

    @Test
    public void testRangeRequest() throws Exception {
        RangeRequest request = new RangeRequest()
            .setKey("test-key".getBytes(UTF_8));
        RangeRequest expectedRequest = new RangeRequest();
        expectedRequest.copyFrom(request);
        expectedRequest.setHeader().copyFrom(newRoutingHeader(streamId, rangeId, routingKey));
        RangeResponse response = this.channel.getTableService()
            .range(request).get();

        assertEquals(expectedRequest, receivedRequest.get());
        assertEquals(expectedRequest.getHeader(), response.getHeader().getRoutingHeader());
    }

    @Test
    public void testDeleteRangeRequest() throws Exception {
        DeleteRangeRequest request = new DeleteRangeRequest()
            .setKey("test-key".getBytes(UTF_8));
        DeleteRangeRequest expectedRequest = new DeleteRangeRequest();
        expectedRequest.copyFrom(request);
        expectedRequest.setHeader().copyFrom(newRoutingHeader(streamId, rangeId, routingKey));
        DeleteRangeResponse response = this.channel.getTableService()
            .delete(request).get();

        assertEquals(expectedRequest, receivedRequest.get());
        assertEquals(expectedRequest.getHeader(), response.getHeader().getRoutingHeader());
    }

    @Test
    public void testIncrementRequest() throws Exception {
        IncrementRequest request = new IncrementRequest()
            .setKey("test-key".getBytes(UTF_8));
        IncrementRequest expectedRequest = new IncrementRequest();
        expectedRequest.copyFrom(request);
        expectedRequest.setHeader().copyFrom(newRoutingHeader(streamId, rangeId, routingKey));
        IncrementResponse response = this.channel.getTableService()
            .increment(request).get();

        assertEquals(expectedRequest, receivedRequest.get());
        assertEquals(expectedRequest.getHeader(), response.getHeader().getRoutingHeader());
    }

    @Test
    public void testTxnRequest() throws Exception {
        TxnRequest request = new TxnRequest();
        TxnRequest expectedRequest = new TxnRequest();
        expectedRequest.copyFrom(request);
        expectedRequest.setHeader().copyFrom(newRoutingHeader(streamId, rangeId, routingKey));
        TxnResponse response = this.channel.getTableService().txn(request).get();

        assertEquals(expectedRequest, receivedRequest.get());
        assertEquals(expectedRequest.getHeader(), response.getHeader().getRoutingHeader());
    }

    @Test
    public void testForwardOriginalInputStreamWhenInterceptionFails() throws Exception {
        byte[] requestBytes = new byte[] { 4, 1, 2, 3 };
        CapturingClientCall delegateCall = new CapturingClientCall();
        RoutingHeaderProxyInterceptor interceptor = new RoutingHeaderProxyInterceptor();

        ClientCall<InputStream, InputStream> interceptedCall = interceptor.interceptCall(
            proxyTxnMethod(),
            CallOptions.DEFAULT,
            new CapturingChannel(delegateCall));

        Metadata headers = new Metadata();
        headers.put(SID_METADATA_KEY, 1026L);
        headers.put(RID_METADATA_KEY, 1030L);
        headers.put(RK_METADATA_KEY, "txn-key".getBytes(UTF_8));

        ByteArrayInputStream originalMessage = new ByteArrayInputStream(requestBytes);
        interceptedCall.start(new ClientCall.Listener<InputStream>() { }, headers);
        interceptedCall.sendMessage(originalMessage);

        assertNotSame(originalMessage, delegateCall.message);
        assertArrayEquals(requestBytes, delegateCall.message.readAllBytes());
    }

    private static MethodDescriptor<InputStream, InputStream> proxyTxnMethod() {
        return MethodDescriptor.newBuilder(
                IdentityInputStreamMarshaller.of(),
                IdentityInputStreamMarshaller.of())
            .setFullMethodName(TableServiceGrpc.getTxnMethod().getFullMethodName())
            .setType(TableServiceGrpc.getTxnMethod().getType())
            .build();
    }

    private static class CapturingChannel extends Channel {

        private final CapturingClientCall call;

        CapturingChannel(CapturingClientCall call) {
            this.call = call;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                MethodDescriptor<ReqT, RespT> methodDescriptor,
                CallOptions callOptions) {
            return (ClientCall<ReqT, RespT>) call;
        }

        @Override
        public String authority() {
            return "test-authority";
        }
    }

    private static class CapturingClientCall extends ClientCall<InputStream, InputStream> {

        private InputStream message;

        @Override
        public void start(Listener<InputStream> responseListener, Metadata headers) {
        }

        @Override
        public void request(int numMessages) {
        }

        @Override
        public void cancel(String message, Throwable cause) {
        }

        @Override
        public void halfClose() {
        }

        @Override
        public void sendMessage(InputStream message) {
            this.message = message;
        }
    }

}
