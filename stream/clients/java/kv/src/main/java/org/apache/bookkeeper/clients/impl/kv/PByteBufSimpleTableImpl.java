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

import static org.apache.bookkeeper.clients.impl.kv.KvUtils.populateProtoCompare;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.populateProtoRequest;
import static org.apache.bookkeeper.common.util.ListenableFutures.fromListenableFuture;
import static org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.getDeleteMethod;
import static org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.getIncrementMethod;
import static org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.getPutMethod;
import static org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.getRangeMethod;
import static org.apache.bookkeeper.stream.proto.kv.rpc.TableServiceGrpc.getTxnMethod;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.RK_METADATA_KEY;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.SID_METADATA_KEY;

import com.google.common.collect.Lists;
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
import lombok.CustomLog;
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
import org.apache.bookkeeper.clients.exceptions.InternalServerException;
import org.apache.bookkeeper.clients.utils.RetryUtils;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.stream.proto.StreamProperties;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * A {@link PTable} implementation using simple grpc calls.
 */
@CustomLog
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

    private void populateRoutingHeader(RoutingHeader header, ByteBuf pKey) {
        header.setStreamId(streamId);
        // Use a slice so this header's rKey has its own readerIndex independent of the
        // request's key/value fields when those alias the same underlying ByteBuf:
        // lightproto's serializer calls ByteBuf#writeBytes(src) which advances src's
        // readerIndex, so two fields backed by the same ByteBuf would clobber each other.
        header.setRKey(pKey.slice());
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
        return retryUtils.execute(() -> {
            RangeRequest request = KvUtils.newRangeRequest(lKey, option);
            populateRoutingHeader(request.setHeader(), pKey);
            return fromListenableFuture(
                ClientCalls.futureUnaryCall(
                    getChannel(pKey).newCall(getRangeMethod(), getCallOptions()),
                    request));
        })
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
        return retryUtils.execute(() -> {
            PutRequest request = KvUtils.newPutRequest(lKey, value, option);
            populateRoutingHeader(request.setHeader(), pKey);
            return fromListenableFuture(
                ClientCalls.futureUnaryCall(
                    getChannel(pKey).newCall(getPutMethod(), getCallOptions()),
                    request));
        })
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
        return retryUtils.execute(() -> {
            DeleteRangeRequest request = KvUtils.newDeleteRequest(lKey, option);
            populateRoutingHeader(request.setHeader(), pKey);
            return fromListenableFuture(
                ClientCalls.futureUnaryCall(
                    getChannel(pKey).newCall(getDeleteMethod(), getCallOptions()),
                    request));
        })
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
        return retryUtils.execute(() -> {
            IncrementRequest request = KvUtils.newIncrementRequest(lKey, amount, option);
            populateRoutingHeader(request.setHeader(), pKey);
            return fromListenableFuture(
                ClientCalls.futureUnaryCall(
                    getChannel(pKey).newCall(getIncrementMethod(), getCallOptions()),
                    request));
        })
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
        private final List<CompareOp<ByteBuf, ByteBuf>> compareOps;
        private final List<Op<ByteBuf, ByteBuf>> successOps;
        private final List<Op<ByteBuf, ByteBuf>> failureOps;

        TxnImpl(ByteBuf pKey) {
            this.pKey = pKey.retain();
            this.compareOps = Lists.newArrayList();
            this.successOps = Lists.newArrayList();
            this.failureOps = Lists.newArrayList();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Txn<ByteBuf, ByteBuf> If(CompareOp... cmps) {
            for (CompareOp<ByteBuf, ByteBuf> cmp : cmps) {
                compareOps.add(cmp);
            }
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Txn<ByteBuf, ByteBuf> Then(Op... ops) {
            for (Op<ByteBuf, ByteBuf> op : ops) {
                successOps.add(op);
            }
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Txn<ByteBuf, ByteBuf> Else(Op... ops) {
            for (Op<ByteBuf, ByteBuf> op : ops) {
                failureOps.add(op);
            }
            return this;
        }

        // Serializing a request drains the ByteBuf slices stored in it, so a request instance
        // must not be reused across RPC attempts: a retried attempt would send a corrupted
        // request. Build a fresh request per attempt, like put/get/delete/increment above.
        private TxnRequest newTxnRequest() {
            TxnRequest txnRequest = new TxnRequest();
            for (CompareOp<ByteBuf, ByteBuf> cmp : compareOps) {
                populateProtoCompare(txnRequest.addCompare(), cmp);
            }
            for (Op<ByteBuf, ByteBuf> op : successOps) {
                populateProtoRequest(txnRequest.addSuccess(), op);
            }
            for (Op<ByteBuf, ByteBuf> op : failureOps) {
                populateProtoRequest(txnRequest.addFailure(), op);
            }
            populateRoutingHeader(txnRequest.setHeader(), pKey);
            return txnRequest;
        }

        @Override
        public CompletableFuture<TxnResult<ByteBuf, ByteBuf>> commit() {
            return retryUtils.execute(() -> fromListenableFuture(
            ClientCalls.futureUnaryCall(
                getChannel(pKey).newCall(getTxnMethod(), getCallOptions()),
                newTxnRequest())
            ))
            .thenCompose(response -> {
                if (StatusCode.SUCCESS != response.getHeader().getCode()) {
                    // A server-side error must not be conflated with a failed txn compare.
                    return FutureUtils.exception(new InternalServerException(
                        "Encountered internal server exception : code = " + response.getHeader().getCode()));
                }
                return FutureUtils.value(KvUtils.newKvTxnResult(response, resultFactory, kvFactory));
            })
            .whenComplete((ignored, cause) -> {
                ReferenceCountUtil.release(pKey);
                for (CompareOp<ByteBuf, ByteBuf> cmp : compareOps) {
                    closeResource(cmp);
                }
                for (Op<ByteBuf, ByteBuf> op : successOps) {
                    closeResource(op);
                }
                for (Op<ByteBuf, ByteBuf> op : failureOps) {
                    closeResource(op);
                }
            });
        }

        private void closeResource(AutoCloseable resource) {
            try {
                resource.close();
            } catch (Exception e) {
                log.warn().attr("resource", resource).exception(e).log("Fail to close resource");
            }
        }
    }

    @Override
    protected PByteBufSimpleTableImpl build(Channel channel, CallOptions callOptions) {
        return new PByteBufSimpleTableImpl(streamProps, channel, callOptions, retryUtils);
    }
}
