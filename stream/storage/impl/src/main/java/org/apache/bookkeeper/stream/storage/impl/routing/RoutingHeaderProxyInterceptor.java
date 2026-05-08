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

import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.RID_METADATA_KEY;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.RK_METADATA_KEY;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.SID_METADATA_KEY;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import lombok.CustomLog;
import lombok.Data;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.commons.codec.binary.Hex;

/**
 * A client interceptor that intercepting kv rpcs to attach routing information.
 */
@CustomLog
public class RoutingHeaderProxyInterceptor implements ClientInterceptor {

    /**
     * Table request mutator that mutates a table service rpc request to attach
     * the routing information.
     */
    private interface TableRequestMutator<ReqT> {

        /**
         * Mutate the provided <tt>request</tt> to attach the given routing information.
         *
         * @param request table request to be mutated
         * @param sid stream id
         * @param rid range id
         * @param rk routing key
         * @return the mutated request
         */
        ReqT intercept(ReqT request,
                       Long sid,
                       Long rid,
                       byte[] rk);

    }

    private static RoutingHeader newRoutingHeader(RoutingHeader header,
                                                  Long sid,
                                                  Long rid,
                                                  byte[] rk) {
        RoutingHeader newHeader = new RoutingHeader();
        newHeader.copyFrom(header);
        newHeader.setStreamId(sid);
        newHeader.setRangeId(rid);
        newHeader.setRKey(rk);
        return newHeader;
    }

    private static final TableRequestMutator<PutRequest> PUT_INTERCEPTOR =
        (request, sid, rid, rk) -> {
            PutRequest mutated = new PutRequest();
            mutated.copyFrom(request);
            mutated.setHeader().copyFrom(newRoutingHeader(request.getHeader(), sid, rid, rk));
            return mutated;
        };

    private static final TableRequestMutator<RangeRequest> RANGE_INTERCEPTOR =
        (request, sid, rid, rk) -> {
            RangeRequest mutated = new RangeRequest();
            mutated.copyFrom(request);
            mutated.setHeader().copyFrom(newRoutingHeader(request.getHeader(), sid, rid, rk));
            return mutated;
        };

    private static final TableRequestMutator<DeleteRangeRequest> DELETE_INTERCEPTOR =
        (request, sid, rid, rk) -> {
            DeleteRangeRequest mutated = new DeleteRangeRequest();
            mutated.copyFrom(request);
            mutated.setHeader().copyFrom(newRoutingHeader(request.getHeader(), sid, rid, rk));
            return mutated;
        };

    private static final TableRequestMutator<IncrementRequest> INCR_INTERCEPTOR =
        (request, sid, rid, rk) -> {
            IncrementRequest mutated = new IncrementRequest();
            mutated.copyFrom(request);
            mutated.setHeader().copyFrom(newRoutingHeader(request.getHeader(), sid, rid, rk));
            return mutated;
        };

    private static final TableRequestMutator<TxnRequest> TXN_INTERCEPTOR =
        (request, sid, rid, rk) -> {
            TxnRequest mutated = new TxnRequest();
            mutated.copyFrom(request);
            mutated.setHeader().copyFrom(newRoutingHeader(request.getHeader(), sid, rid, rk));
            return mutated;
        };

    /**
     * Parser that creates a new instance of a lightproto message from a byte array.
     */
    private interface LightProtoParser<T> {
        T parseFrom(byte[] data);
    }

    /**
     * Serializer that converts a lightproto message to a byte array.
     */
    private interface LightProtoSerializer<T> {
        byte[] toByteArray(T msg);
    }

    @Data(staticConstructor = "of")
    private static class InterceptorDescriptor<T> {

        private final Class<T> clz;
        private final Supplier<T> factory;
        private final LightProtoParser<T> parser;
        private final LightProtoSerializer<T> serializer;
        private final TableRequestMutator<T> interceptor;

    }

    private static <T> InterceptorDescriptor<T> descriptor(
        Class<T> clz,
        Supplier<T> factory,
        LightProtoParser<T> parser,
        LightProtoSerializer<T> serializer,
        TableRequestMutator<T> interceptor
    ) {
        return InterceptorDescriptor.of(clz, factory, parser, serializer, interceptor);
    }

    private static Map<String, InterceptorDescriptor<?>> kvRpcMethods = new HashMap<>();
    private static PutRequest parsePutRequest(byte[] bytes) {
        PutRequest m = new PutRequest();
        m.parseFrom(bytes);
        return m;
    }

    private static RangeRequest parseRangeRequest(byte[] bytes) {
        RangeRequest m = new RangeRequest();
        m.parseFrom(bytes);
        return m;
    }

    private static DeleteRangeRequest parseDeleteRangeRequest(byte[] bytes) {
        DeleteRangeRequest m = new DeleteRangeRequest();
        m.parseFrom(bytes);
        return m;
    }

    private static IncrementRequest parseIncrementRequest(byte[] bytes) {
        IncrementRequest m = new IncrementRequest();
        m.parseFrom(bytes);
        return m;
    }

    private static TxnRequest parseTxnRequest(byte[] bytes) {
        TxnRequest m = new TxnRequest();
        m.parseFrom(bytes);
        return m;
    }

    static {
        kvRpcMethods.put(
            TableServiceGrpc.getPutMethod().getFullMethodName(),
            descriptor(
                PutRequest.class,
                PutRequest::new,
                RoutingHeaderProxyInterceptor::parsePutRequest,
                PutRequest::toByteArray,
                PUT_INTERCEPTOR
            )
        );
        kvRpcMethods.put(
            TableServiceGrpc.getRangeMethod().getFullMethodName(),
            descriptor(
                RangeRequest.class,
                RangeRequest::new,
                RoutingHeaderProxyInterceptor::parseRangeRequest,
                RangeRequest::toByteArray,
                RANGE_INTERCEPTOR
            )
        );
        kvRpcMethods.put(
            TableServiceGrpc.getDeleteMethod().getFullMethodName(),
            descriptor(
                DeleteRangeRequest.class,
                DeleteRangeRequest::new,
                RoutingHeaderProxyInterceptor::parseDeleteRangeRequest,
                DeleteRangeRequest::toByteArray,
                DELETE_INTERCEPTOR
            )
        );
        kvRpcMethods.put(
            TableServiceGrpc.getIncrementMethod().getFullMethodName(),
            descriptor(
                IncrementRequest.class,
                IncrementRequest::new,
                RoutingHeaderProxyInterceptor::parseIncrementRequest,
                IncrementRequest::toByteArray,
                INCR_INTERCEPTOR
            )
        );
        kvRpcMethods.put(
            TableServiceGrpc.getTxnMethod().getFullMethodName(),
            descriptor(
                TxnRequest.class,
                TxnRequest::new,
                RoutingHeaderProxyInterceptor::parseTxnRequest,
                TxnRequest::toByteArray,
                TXN_INTERCEPTOR
            )
        );
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions,
                                                               Channel next) {
        log.trace()
            .attr("method", method.getFullMethodName())
            .attr("requestMarshaller", method.getRequestMarshaller())
            .attr("responseMarshaller", method.getResponseMarshaller())
            .log("Intercepting method");
        InterceptorDescriptor<?> descriptor = kvRpcMethods.get(method.getFullMethodName());
        return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

            private Long rid = null;
            private Long sid = null;
            private byte[] rk = null;

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                // capture routing information from headers
                sid = headers.get(SID_METADATA_KEY);
                rid = headers.get(RID_METADATA_KEY);
                rk  = headers.get(RK_METADATA_KEY);
                log.trace()
                    .attr("streamId", sid)
                    .attr("rangeId", rid)
                    .attr("routingKey", rk)
                    .log("Intercepting request with header");

                delegate().start(responseListener, headers);
            }

            @Override
            public void sendMessage(ReqT message) {
                ReqT interceptedMessage;
                if (null == rid || null == sid || null == rk || null == descriptor) {
                    // we don't have enough information to form the new routing header
                    // we simply copy the bytes and generate a new message to forward
                    // the request payload
                    interceptedMessage = interceptMessage(method, message);
                } else {
                    interceptedMessage = interceptMessage(
                        method,
                        descriptor,
                        message,
                        sid,
                        rid,
                        rk
                    );
                }
                delegate().sendMessage(interceptedMessage);
            }
        };
    }

    private <ReqT, RespT> ReqT interceptMessage(MethodDescriptor<ReqT, RespT> method, ReqT message) {
        InputStream is = method.getRequestMarshaller().stream(message);
        int bytes;
        try {
            bytes = is.available();
        } catch (IOException e) {
            log.warn().exception(e).log("Encountered exceptions in getting available bytes of message");
            throw new RuntimeException("Encountered exception in intercepting message", e);
        }
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
        try {
            buffer.writeBytes(is, bytes);
        } catch (IOException e) {
            log.warn().exception(e).log("Encountered exceptions in transferring bytes to the buffer");
            ReferenceCountUtil.release(buffer);
            throw new RuntimeException("Encountered exceptions in transferring bytes to the buffer", e);
        }
        return method
            .getRequestMarshaller()
            .parse(new ByteBufInputStream(buffer, true));
    }

    private <ReqT, TableReqT> ReqT interceptMessage(
        MethodDescriptor<ReqT, ?> method,
        InterceptorDescriptor<TableReqT> descriptor,
        ReqT message,
        Long sid,
        Long rid,
        byte[] rk
    ) {
        if (null == descriptor) {
            return message;
        } else {
            try {
                return interceptTableRequest(method, descriptor, message, sid, rid, rk);
            } catch (Throwable t) {
                log.error()
                    .attr("streamId", sid)
                    .attr("rangeId", rid)
                    .attr("routingKey", Hex.encodeHexString(rk))
                    .exception(t)
                    .log("Failed to intercept table request");
                return message;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <ReqT, TableReqT> ReqT interceptTableRequest(
        MethodDescriptor<ReqT, ?> method,
        InterceptorDescriptor<TableReqT> interceptor,
        ReqT message,
        Long sid, Long rid, byte[] rk
    ) throws IOException {
        // put request
        TableReqT request;
        if (message.getClass() == interceptor.getClz()) {
            request = (TableReqT) message;
        } else {
            InputStream is = method.getRequestMarshaller().stream(message);
            byte[] bytes = is.readAllBytes();
            request = interceptor.getParser().parseFrom(bytes);
        }
        TableReqT interceptedMessage = interceptor.getInterceptor().intercept(
            request, sid, rid, rk
        );
        if (message.getClass() == interceptor.getClz()) {
            return (ReqT) interceptedMessage;
        } else {
            byte[] reqBytes = interceptor.getSerializer().toByteArray(interceptedMessage);
            return method.getRequestMarshaller().parse(new ByteArrayInputStream(reqBytes));

        }
    }
}
