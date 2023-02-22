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

package org.apache.bookkeeper.clients.impl.kv;

import static org.apache.bookkeeper.clients.impl.kv.KvUtils.toProtoCompare;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.toProtoRequest;
import static org.apache.bookkeeper.common.util.ListenableFutures.fromListenableFuture;
import static org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.getDeleteMethod;
import static org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.getIncrementMethod;
import static org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.getPutMethod;
import static org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.getRangeMethod;
import static org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.getTxnMethod;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.RK_METADATA_KEY;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.SID_METADATA_KEY;

import com.google.common.collect.Lists;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ClientInterceptors.CheckedForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ReferenceCountUtil;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.Txn;
import org.apache.bookkeeper.api.kv.impl.op.OpFactoryImpl;
import org.apache.bookkeeper.api.kv.impl.result.KeyValueFactory;
import org.apache.bookkeeper.api.kv.impl.result.ResultFactory;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.Op;
import org.apache.bookkeeper.api.kv.op.OpFactory;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.IncrementOption;
import org.apache.bookkeeper.api.kv.options.PutOption;
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.api.kv.result.IncrementResult;
import org.apache.bookkeeper.api.kv.result.PutResult;
import org.apache.bookkeeper.api.kv.result.RangeResult;
import org.apache.bookkeeper.api.kv.result.TxnResult;
import org.apache.bookkeeper.clients.utils.RetryUtils;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;

/**
 * A {@link PTable} implementation using simple grpc calls.
 */
@Slf4j
public class PByteBufSimpleTableImpl
    extends AbstractStub<PByteBufSimpleTableImpl>
    implements PTable<ByteBuf, ByteBuf> {

    private static class RoutingHeaderInterceptor implements ClientInterceptor {

        private final long streamId;
        private final ByteBuf rKey;

        RoutingHeaderInterceptor(long streamId, ByteBuf rKey) {
            this.streamId = streamId;
            this.rKey = rKey;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                   CallOptions callOptions,
                                                                   Channel next) {
            return new CheckedForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                @Override
                protected void checkedStart(Listener<RespT> responseListener, Metadata headers) throws Exception {
                    headers.put(SID_METADATA_KEY, streamId);
                    headers.put(RK_METADATA_KEY, ByteBufUtil.getBytes(rKey));
                    delegate().start(responseListener, headers);
                }
            };
        }
    }

    private final OpFactory<ByteBuf, ByteBuf> opFactory;
    private final ResultFactory<ByteBuf, ByteBuf> resultFactory;
    private final KeyValueFactory<ByteBuf, ByteBuf> kvFactory;
    private final StreamProperties streamProps;
    private final long streamId;
    private final RetryUtils retryUtils;

    public PByteBufSimpleTableImpl(StreamProperties streamProps,
                                   Channel channel,
                                   CallOptions callOptions,
                                   RetryUtils retryUtils) {
        super(channel, callOptions);
        this.streamProps = streamProps;
        this.streamId = streamProps.getStreamId();
        this.opFactory = new OpFactoryImpl<>();
        this.resultFactory = new ResultFactory<>();
        this.kvFactory = new KeyValueFactory<>();
        this.retryUtils = retryUtils;
    }

    private RoutingHeader.Builder newRoutingHeader(ByteBuf pKey) {
        return RoutingHeader.newBuilder()
            .setStreamId(streamId)
            .setRKey(UnsafeByteOperations.unsafeWrap(pKey.nioBuffer()));
    }

    private Channel getChannel(ByteBuf pKey) {
        RoutingHeaderInterceptor interceptor = new RoutingHeaderInterceptor(streamId, pKey);
        return ClientInterceptors.intercept(getChannel(), interceptor);
    }

    @Override
    public CompletableFuture<RangeResult<ByteBuf, ByteBuf>> get(
        ByteBuf pKey, ByteBuf lKey, RangeOption<ByteBuf> option
    ) {
        pKey.retain();
        lKey.retain();
        if (null != option.endKey()) {
            option.endKey().retain();
        }
        return retryUtils.execute(() -> fromListenableFuture(
            ClientCalls.futureUnaryCall(
                getChannel(pKey).newCall(getRangeMethod(), getCallOptions()),
                KvUtils.newRangeRequest(lKey, option)
                    .setHeader(newRoutingHeader(pKey))
                    .build())
        ))
        .thenApply(response -> KvUtils.newRangeResult(response, resultFactory, kvFactory))
        .whenComplete((value, cause) -> {
            ReferenceCountUtil.release(pKey);
            ReferenceCountUtil.release(lKey);
            if (null != option.endKey()) {
                ReferenceCountUtil.release(option.endKey());
            }
        });
    }

    @Override
    public CompletableFuture<PutResult<ByteBuf, ByteBuf>> put(
        ByteBuf pKey, ByteBuf lKey, ByteBuf value, PutOption<ByteBuf> option
    ) {
        pKey.retain();
        lKey.retain();
        value.retain();
        return retryUtils.execute(() -> fromListenableFuture(
            ClientCalls.futureUnaryCall(
                getChannel(pKey).newCall(getPutMethod(), getCallOptions()),
                KvUtils.newPutRequest(lKey, value, option)
                    .setHeader(newRoutingHeader(pKey))
                    .build())
            ))
            .thenApply(response -> KvUtils.newPutResult(response, resultFactory, kvFactory))
            .whenComplete((ignored, cause) -> {
                ReferenceCountUtil.release(pKey);
                ReferenceCountUtil.release(lKey);
                ReferenceCountUtil.release(value);
            });
    }

    @Override
    public CompletableFuture<DeleteResult<ByteBuf, ByteBuf>> delete(
        ByteBuf pKey, ByteBuf lKey, DeleteOption<ByteBuf> option
    ) {
        pKey.retain();
        lKey.retain();
        if (null != option.endKey()) {
            option.endKey().retain();
        }
        return retryUtils.execute(() -> fromListenableFuture(
            ClientCalls.futureUnaryCall(
                getChannel(pKey).newCall(getDeleteMethod(), getCallOptions()),
                KvUtils.newDeleteRequest(lKey, option)
                    .setHeader(newRoutingHeader(pKey))
                    .build())
        ))
        .thenApply(response -> KvUtils.newDeleteResult(response, resultFactory, kvFactory))
        .whenComplete((ignored, cause) -> {
            ReferenceCountUtil.release(pKey);
            ReferenceCountUtil.release(lKey);
            if (null != option.endKey()) {
                ReferenceCountUtil.release(option.endKey());
            }
        });
    }

    @Override
    public CompletableFuture<IncrementResult<ByteBuf, ByteBuf>> increment(
        ByteBuf pKey, ByteBuf lKey, long amount, IncrementOption<ByteBuf> option
    ) {
        pKey.retain();
        lKey.retain();
        return retryUtils.execute(() -> fromListenableFuture(
            ClientCalls.futureUnaryCall(
                getChannel(pKey).newCall(getIncrementMethod(), getCallOptions()),
                KvUtils.newIncrementRequest(lKey, amount, option)
                    .setHeader(newRoutingHeader(pKey))
                    .build())
        ))
        .thenApply(response -> KvUtils.newIncrementResult(response, resultFactory, kvFactory))
        .whenComplete((ignored, cause) -> {
            ReferenceCountUtil.release(pKey);
            ReferenceCountUtil.release(lKey);
        });
    }

    @Override
    public Txn<ByteBuf, ByteBuf> txn(ByteBuf pKey) {
        return new TxnImpl(pKey);
    }

    @Override
    public OpFactory<ByteBuf, ByteBuf> opFactory() {
        return opFactory;
    }

    @Override
    public void close() {
        // no-op
    }

    //
    // Txn Implementation
    //

    class TxnImpl implements Txn<ByteBuf, ByteBuf> {

        private final ByteBuf pKey;
        private final TxnRequest.Builder txnBuilder;
        private final List<AutoCloseable> resourcesToRelease;

        TxnImpl(ByteBuf pKey) {
            this.pKey = pKey.retain();
            this.txnBuilder = TxnRequest.newBuilder();
            this.resourcesToRelease = Lists.newArrayList();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Txn<ByteBuf, ByteBuf> If(CompareOp... cmps) {
            for (CompareOp<ByteBuf, ByteBuf> cmp : cmps) {
                txnBuilder.addCompare(toProtoCompare(cmp));
                resourcesToRelease.add(cmp);
            }
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Txn<ByteBuf, ByteBuf> Then(Op... ops) {
            for (Op<ByteBuf, ByteBuf> op : ops) {
                txnBuilder.addSuccess(toProtoRequest(op));
                resourcesToRelease.add(op);
            }
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Txn<ByteBuf, ByteBuf> Else(Op... ops) {
            for (Op<ByteBuf, ByteBuf> op : ops) {
                txnBuilder.addFailure(toProtoRequest(op));
                resourcesToRelease.add(op);
            }
            return this;
        }

        @Override
        public CompletableFuture<TxnResult<ByteBuf, ByteBuf>> commit() {
            return retryUtils.execute(() -> fromListenableFuture(
            ClientCalls.futureUnaryCall(
                getChannel(pKey).newCall(getTxnMethod(), getCallOptions()),
                txnBuilder.setHeader(newRoutingHeader(pKey)).build())
            ))
            .thenApply(response -> KvUtils.newKvTxnResult(response, resultFactory, kvFactory))
            .whenComplete((ignored, cause) -> {
                ReferenceCountUtil.release(pKey);
                for (AutoCloseable resource : resourcesToRelease) {
                    closeResource(resource);
                }
            });
        }

        private void closeResource(AutoCloseable resource) {
            try {
                resource.close();
            } catch (Exception e) {
                log.warn("Fail to close resource {}", resource, e);
            }
        }
    }

    @Override
    protected PByteBufSimpleTableImpl build(Channel channel, CallOptions callOptions) {
        return new PByteBufSimpleTableImpl(streamProps, channel, callOptions, retryUtils);
    }
}
