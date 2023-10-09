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

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.google.protobuf.UnsafeByteOperations;
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
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
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

    private static RoutingHeader.Builder newRoutingHeaderBuilder(RoutingHeader header,
                                                                 Long sid,
                                                                 Long rid,
                                                                 byte[] rk) {
        return RoutingHeader.newBuilder(header)
                .setStreamId(sid)
                .setRangeId(rid)
                .setRKey(UnsafeByteOperations.unsafeWrap(rk));
    }

    private static final TableRequestMutator<PutRequest> PUT_INTERCEPTOR =
        (request, sid, rid, rk) -> PutRequest.newBuilder(request)
            .setHeader(newRoutingHeaderBuilder(request.getHeader(), sid, rid, rk))
            .build();

    private static final TableRequestMutator<RangeRequest> RANGE_INTERCEPTOR =
        (request, sid, rid, rk) -> RangeRequest.newBuilder(request)
            .setHeader(newRoutingHeaderBuilder(request.getHeader(), sid, rid, rk))
            .build();

    private static final TableRequestMutator<DeleteRangeRequest> DELETE_INTERCEPTOR =
        (request, sid, rid, rk) -> DeleteRangeRequest.newBuilder(request)
            .setHeader(newRoutingHeaderBuilder(request.getHeader(), sid, rid, rk))
            .build();

    private static final TableRequestMutator<IncrementRequest> INCR_INTERCEPTOR =
        (request, sid, rid, rk) -> IncrementRequest.newBuilder(request)
            .setHeader(newRoutingHeaderBuilder(request.getHeader(), sid, rid, rk))
            .build();

    private static final TableRequestMutator<TxnRequest> TXN_INTERCEPTOR =
        (request, sid, rid, rk) -> TxnRequest.newBuilder(request)
            .setHeader(newRoutingHeaderBuilder(request.getHeader(), sid, rid, rk))
            .build();

    @Data(staticConstructor = "of")
    private static class InterceptorDescriptor<T extends MessageLite> {

        private final Class<T> clz;
        private final Parser<T> parser;
        private final TableRequestMutator<T> interceptor;

    }

    private static Map<String, InterceptorDescriptor<?>> kvRpcMethods = new HashMap<>();
    static {
        kvRpcMethods.put(
            TableServiceGrpc.getPutMethod().getFullMethodName(),
            InterceptorDescriptor.of(
                PutRequest.class, PutRequest.parser(), PUT_INTERCEPTOR
            )
        );
        kvRpcMethods.put(
            TableServiceGrpc.getRangeMethod().getFullMethodName(),
            InterceptorDescriptor.of(
                RangeRequest.class, RangeRequest.parser(), RANGE_INTERCEPTOR
            )
        );
        kvRpcMethods.put(
            TableServiceGrpc.getDeleteMethod().getFullMethodName(),
            InterceptorDescriptor.of(
                DeleteRangeRequest.class, DeleteRangeRequest.parser(), DELETE_INTERCEPTOR
            )
        );
        kvRpcMethods.put(
            TableServiceGrpc.getIncrementMethod().getFullMethodName(),
            InterceptorDescriptor.of(
                IncrementRequest.class, IncrementRequest.parser(), INCR_INTERCEPTOR
            )
        );
        kvRpcMethods.put(
            TableServiceGrpc.getTxnMethod().getFullMethodName(),
            InterceptorDescriptor.of(
                TxnRequest.class, TxnRequest.parser(), TXN_INTERCEPTOR
            )
        );
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions,
                                                               Channel next) {
        if (log.isTraceEnabled()) {
            log.trace("Intercepting method {} : req marshaller = {}, resp marshaller = {}",
                method.getFullMethodName(),
                method.getRequestMarshaller(),
                method.getResponseMarshaller());
        }
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
                if (log.isTraceEnabled()) {
                    log.trace("Intercepting request with header : sid = {}, rid = {}, rk = {}",
                        sid, rid, rk);
                }

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
            log.warn("Encountered exceptions in getting available bytes of message", e);
            throw new RuntimeException("Encountered exception in intercepting message", e);
        }
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
        try {
            buffer.writeBytes(is, bytes);
        } catch (IOException e) {
            log.warn("Encountered exceptions in transferring bytes to the buffer", e);
            ReferenceCountUtil.release(buffer);
            throw new RuntimeException("Encountered exceptions in transferring bytes to the buffer", e);
        }
        return method
            .getRequestMarshaller()
            .parse(new ByteBufInputStream(buffer, true));
    }

    private <ReqT, TableReqT extends MessageLite> ReqT interceptMessage(
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
                log.error("Failed to intercept table request (sid = {}, rid = {}, rk = {}) : ",
                    sid, rid, Hex.encodeHexString(rk), t);
                return message;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <ReqT, TableReqT extends MessageLite> ReqT interceptTableRequest(
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
            request = interceptor.getParser().parseFrom(is);
        }
        TableReqT interceptedMessage = interceptor.getInterceptor().intercept(
            request, sid, rid, rk
        );
        if (message.getClass() == interceptor.getClz()) {
            return (ReqT) interceptedMessage;
        } else {
            byte[] reqBytes = new byte[interceptedMessage.getSerializedSize()];
            interceptedMessage.writeTo(CodedOutputStream.newInstance(reqBytes));
            return method.getRequestMarshaller().parse(new ByteArrayInputStream(reqBytes));

        }
    }
}
