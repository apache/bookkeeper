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

package org.apache.bookkeeper.clients.impl.kv;

import static org.apache.bookkeeper.clients.impl.kv.KvUtils.toProtoCompare;
import static org.apache.bookkeeper.clients.impl.kv.KvUtils.toProtoRequest;

import com.google.common.collect.Lists;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.PTable;
import org.apache.bookkeeper.api.kv.Txn;
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
import org.apache.bookkeeper.clients.impl.container.StorageContainerChannel;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.stream.proto.RangeProperties;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;

/**
 * A range of a table.
 */
@Slf4j
class PByteBufTableRangeImpl implements PTable<ByteBuf, ByteBuf> {

    private final long streamId;
    private final RangeProperties rangeProps;
    private final StorageContainerChannel scChannel;
    private final ScheduledExecutorService executor;
    private final OpFactory<ByteBuf, ByteBuf> opFactory;
    private final ResultFactory<ByteBuf, ByteBuf> resultFactory;
    private final KeyValueFactory<ByteBuf, ByteBuf> kvFactory;
    private final Backoff.Policy backoffPolicy;

    PByteBufTableRangeImpl(long streamId,
                           RangeProperties rangeProps,
                           StorageContainerChannel scChannel,
                           ScheduledExecutorService executor,
                           OpFactory<ByteBuf, ByteBuf> opFactory,
                           ResultFactory<ByteBuf, ByteBuf> resultFactory,
                           KeyValueFactory<ByteBuf, ByteBuf> kvFactory,
                           Backoff.Policy backoffPolicy) {
        this.streamId = streamId;
        this.rangeProps = rangeProps;
        this.scChannel = scChannel;
        this.executor = executor;
        this.opFactory = opFactory;
        this.resultFactory = resultFactory;
        this.kvFactory = kvFactory;
        this.backoffPolicy = backoffPolicy;
    }

    private RoutingHeader.Builder newRoutingHeader(ByteBuf pKey) {
        return RoutingHeader.newBuilder()
            .setStreamId(streamId)
            .setRangeId(rangeProps.getRangeId())
            .setRKey(UnsafeByteOperations.unsafeWrap(pKey.nioBuffer()));
    }

    @Override
    public CompletableFuture<RangeResult<ByteBuf, ByteBuf>> get(
        ByteBuf pKey, ByteBuf lKey, RangeOption<ByteBuf> option) {
        pKey.retain();
        lKey.retain();
        if (null != option.endKey()) {
            option.endKey().retain();
        }
        return RangeRequestProcessor.of(
            KvUtils.newRangeRequest(lKey, option)
                .setHeader(newRoutingHeader(pKey))
                .build(),
            response -> KvUtils.newRangeResult(response, resultFactory, kvFactory),
            scChannel,
            executor,
            backoffPolicy
        ).process().whenComplete((value, cause) -> {
            pKey.release();
            lKey.release();
            if (null != option.endKey()) {
                option.endKey().release();
            }
        });
    }

    @Override
    public CompletableFuture<PutResult<ByteBuf, ByteBuf>> put(ByteBuf pKey,
                                                              ByteBuf lKey,
                                                              ByteBuf value,
                                                              PutOption<ByteBuf> option) {
        pKey.retain();
        lKey.retain();
        value.retain();
        return PutRequestProcessor.of(
            KvUtils.newPutRequest(lKey, value, option)
                .setHeader(newRoutingHeader(pKey))
                .build(),
            response -> KvUtils.newPutResult(response, resultFactory, kvFactory),
            scChannel,
            executor,
            backoffPolicy
        ).process().whenComplete((ignored, cause) -> {
            pKey.release();
            lKey.release();
            value.release();
        });
    }

    @Override
    public CompletableFuture<DeleteResult<ByteBuf, ByteBuf>> delete(ByteBuf pKey,
                                                                    ByteBuf lKey,
                                                                    DeleteOption<ByteBuf> option) {
        pKey.retain();
        lKey.retain();
        if (null != option.endKey()) {
            option.endKey().retain();
        }
        return DeleteRequestProcessor.of(
            KvUtils.newDeleteRequest(lKey, option)
                .setHeader(newRoutingHeader(pKey))
                .build(),
            response -> KvUtils.newDeleteResult(response, resultFactory, kvFactory),
            scChannel,
            executor,
            backoffPolicy
        ).process().whenComplete((ignored, cause) -> {
            pKey.release();
            lKey.release();
            if (null != option.endKey()) {
                option.endKey().release();
            }
        });
    }

    @Override
    public CompletableFuture<IncrementResult<ByteBuf, ByteBuf>> increment(ByteBuf pKey,
                                                                          ByteBuf lKey,
                                                                          long amount,
                                                                          IncrementOption<ByteBuf> option) {
        pKey.retain();
        lKey.retain();
        return IncrementRequestProcessor.of(
            KvUtils.newIncrementRequest(lKey, amount, option)
                .setHeader(newRoutingHeader(pKey))
                .build(),
            response -> KvUtils.newIncrementResult(response, resultFactory, kvFactory),
            scChannel,
            executor,
            backoffPolicy
        ).process().whenComplete((ignored, cause) -> {
            pKey.release();
            lKey.release();
        });
    }

    @Override
    public Txn<ByteBuf, ByteBuf> txn(ByteBuf pKey) {
        return new TxnImpl(pKey);
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public OpFactory<ByteBuf, ByteBuf> opFactory() {
        return opFactory;
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
            return TxnRequestProcessor.of(
                txnBuilder.setHeader(newRoutingHeader(pKey)).build(),
                response -> KvUtils.newKvTxnResult(response, resultFactory, kvFactory),
                scChannel,
                executor,
                backoffPolicy
            ).process().whenComplete((ignored, cause) -> {
                pKey.release();
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
}
