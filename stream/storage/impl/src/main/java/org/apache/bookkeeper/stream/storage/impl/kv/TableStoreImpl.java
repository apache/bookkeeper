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
package org.apache.bookkeeper.stream.storage.impl.kv;

import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.fromProtoCompare;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.handleCause;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.newStoreKey;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.processDeleteResult;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.processIncrementResult;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.processPutResult;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.processRangeResult;
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.processTxnResult;

import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.op.DeleteOp;
import org.apache.bookkeeper.api.kv.op.IncrementOp;
import org.apache.bookkeeper.api.kv.op.Op;
import org.apache.bookkeeper.api.kv.op.PutOp;
import org.apache.bookkeeper.api.kv.op.RangeOp;
import org.apache.bookkeeper.api.kv.op.TxnOp;
import org.apache.bookkeeper.api.kv.op.TxnOpBuilder;
import org.apache.bookkeeper.api.kv.options.RangeOptionBuilder;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.api.kv.result.IncrementResult;
import org.apache.bookkeeper.api.kv.result.PutResult;
import org.apache.bookkeeper.api.kv.result.RangeResult;
import org.apache.bookkeeper.api.kv.result.TxnResult;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RequestOp;
import org.apache.bookkeeper.stream.proto.kv.rpc.ResponseHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.storage.api.kv.TableStore;

/**
 * A table store implementation based on {@link org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore}.
 */
@Slf4j
public class TableStoreImpl implements TableStore {

    private final MVCCAsyncStore<byte[], byte[]> store;

    public TableStoreImpl(MVCCAsyncStore<byte[], byte[]> store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<RangeResponse> range(RangeRequest rangeReq) {
        if (log.isTraceEnabled()) {
            log.trace("Received range request {}", rangeReq);
        }
        return doRange(rangeReq)
            .thenApply(result -> {
                try {
                    RangeResponse rangeResp = processRangeResult(
                        rangeReq.getHeader(),
                        result);
                    return rangeResp;
                } finally {
                    result.close();
                }
            })
            .exceptionally(cause -> {
                log.error("Failed to process range request {}", rangeReq, cause);
                return RangeResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setCode(handleCause(cause))
                        .setRoutingHeader(rangeReq.getHeader())
                        .build())
                    .build();
            });
    }

    private CompletableFuture<RangeResult<byte[], byte[]>> doRange(RangeRequest request) {
        RangeOp<byte[], byte[]> op = buildRangeOp(request.getHeader(), request);
        return store.range(op)
            .whenComplete((rangeResult, throwable) -> op.close());
    }

    private RangeOp<byte[], byte[]> buildRangeOp(RoutingHeader header, RangeRequest request) {
        ByteString rKey = header.getRKey();
        ByteString lKey = request.getKey();
        ByteString lEndKey = request.getRangeEnd();
        byte[] storeKey = newStoreKey(rKey, lKey);
        byte[] storeEndKey = null;
        if (null != lEndKey && lEndKey.size() > 0) {
            storeEndKey = newStoreKey(rKey, lEndKey);
        }

        RangeOptionBuilder<byte[]> optionBuilder = store.getOpFactory().optionFactory().newRangeOption();
        if (request.getLimit() > 0) {
            optionBuilder.limit(request.getLimit());
        }
        if (request.getMaxCreateRevision() > 0) {
            optionBuilder.maxCreateRev(request.getMaxCreateRevision());
        }
        if (request.getMaxModRevision() > 0) {
            optionBuilder.maxModRev(request.getMaxModRevision());
        }
        if (request.getMinCreateRevision() > 0) {
            optionBuilder.minCreateRev(request.getMinCreateRevision());
        }
        if (request.getMinModRevision() > 0) {
            optionBuilder.minModRev(request.getMinModRevision());
        }
        if (null != storeEndKey) {
            optionBuilder.endKey(storeEndKey);
        }

        return store.getOpFactory().newRange(
            storeKey,
            optionBuilder.build());
    }

    @Override
    public CompletableFuture<PutResponse> put(PutRequest putReq) {
        return doPut(putReq)
            .thenApply(result -> {
                try {
                    return processPutResult(
                        putReq.getHeader(),
                        result);
                } finally {
                    result.close();
                }
            })
            .exceptionally(cause -> {
                log.error("Failed to process put request {}", putReq, cause);
                return PutResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setCode(handleCause(cause))
                        .setRoutingHeader(putReq.getHeader())
                        .build())
                    .build();
            });
    }

    private CompletableFuture<PutResult<byte[], byte[]>> doPut(PutRequest request) {
        PutOp<byte[], byte[]> op = buildPutOp(request.getHeader(), request);
        return store.put(op)
            .whenComplete((putResult, throwable) -> op.close());
    }

    private PutOp<byte[], byte[]> buildPutOp(RoutingHeader header, PutRequest request) {
        ByteString rKey = header.getRKey();
        ByteString lKey = request.getKey();
        byte[] storeKey = newStoreKey(rKey, lKey);
        return store.getOpFactory().newPut(
            storeKey,
            request.getValue().toByteArray(),
            store.getOpFactory().optionFactory().newPutOption()
                .prevKv(request.getPrevKv())
                .build());
    }

    @Override
    public CompletableFuture<IncrementResponse> incr(IncrementRequest incrementReq) {
        return doIncrement(incrementReq)
            .thenApply(result -> {
                try {
                    return processIncrementResult(
                        incrementReq.getHeader(),
                        result);
                } finally {
                    result.close();
                }
            })
            .exceptionally(cause -> {
                log.error("Failed to process increment request {}", incrementReq, cause);
                return IncrementResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setCode(handleCause(cause))
                        .setRoutingHeader(incrementReq.getHeader())
                        .build())
                    .build();
            });
    }

    private CompletableFuture<IncrementResult<byte[], byte[]>> doIncrement(IncrementRequest request) {
        IncrementOp<byte[], byte[]> op = buildIncrementOp(request.getHeader(), request);
        return store.increment(op)
            .whenComplete((incrementResult, throwable) -> op.close());
    }

    private IncrementOp<byte[], byte[]> buildIncrementOp(RoutingHeader header, IncrementRequest request) {
        ByteString rKey = header.getRKey();
        ByteString lKey = request.getKey();
        byte[] storeKey = newStoreKey(rKey, lKey);
        return store.getOpFactory().newIncrement(
            storeKey,
            request.getAmount(),
            store.getOpFactory().optionFactory().newIncrementOption()
                .getTotal(request.getGetTotal())
                .build());
    }

    @Override
    public CompletableFuture<DeleteRangeResponse> delete(DeleteRangeRequest deleteReq) {
        return doDelete(deleteReq)
            .thenApply(result -> {
                try {
                    return processDeleteResult(
                        deleteReq.getHeader(),
                        result);
                } finally {
                    result.close();
                }
            })
            .exceptionally(cause -> DeleteRangeResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setCode(handleCause(cause))
                    .setRoutingHeader(deleteReq.getHeader())
                    .build())
                .build());
    }

    private CompletableFuture<DeleteResult<byte[], byte[]>> doDelete(DeleteRangeRequest request) {
        DeleteOp<byte[], byte[]> op = buildDeleteOp(request.getHeader(), request);
        return store.delete(op)
            .whenComplete((deleteResult, throwable) -> op.close());
    }

    private DeleteOp<byte[], byte[]> buildDeleteOp(RoutingHeader header, DeleteRangeRequest request) {
        ByteString rKey = header.getRKey();
        ByteString lKey = request.getKey();
        ByteString lEndKey = request.getRangeEnd();
        byte[] storeKey = newStoreKey(rKey, lKey);
        byte[] storeEndKey = null;
        if (null != lEndKey && lEndKey.size() > 0) {
            storeEndKey = newStoreKey(rKey, lEndKey);
        }
        return store.getOpFactory().newDelete(
            storeKey,
            store.getOpFactory().optionFactory().newDeleteOption()
                .prevKv(request.getPrevKv())
                .endKey(storeEndKey)
                .build());
    }

    @Override
    public CompletableFuture<TxnResponse> txn(TxnRequest txnReq) {
        if (log.isTraceEnabled()) {
            log.trace("Received txn request : {}", txnReq);
        }
        return doTxn(txnReq)
            .thenApply(txnResult -> {
                try {
                    return processTxnResult(txnReq.getHeader(), txnResult);
                } finally {
                    txnResult.close();
                }
            })
            .exceptionally(cause -> TxnResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setCode(handleCause(cause))
                    .setRoutingHeader(txnReq.getHeader())
                    .build())
                .build());
    }

    private CompletableFuture<TxnResult<byte[], byte[]>> doTxn(TxnRequest request) {
        TxnOp<byte[], byte[]> op = buildTxnOp(request);
        return store.txn(op)
            .whenComplete((txnResult, throwable) -> op.close());
    }

    private TxnOp<byte[], byte[]> buildTxnOp(TxnRequest request) {
        RoutingHeader header = request.getHeader();
        TxnOpBuilder<byte[], byte[]> txnBuilder = store.getOpFactory().newTxn();
        for (RequestOp requestOp : request.getSuccessList()) {
            txnBuilder.Then(buildTxnOp(header, requestOp));
        }
        for (RequestOp requestOp : request.getFailureList()) {
            txnBuilder.Else(buildTxnOp(header, requestOp));
        }
        for (Compare compare : request.getCompareList()) {
            txnBuilder.If(fromProtoCompare(store.getOpFactory(), header, compare));
        }
        return txnBuilder.build();
    }

    private Op<byte[], byte[]> buildTxnOp(RoutingHeader header, RequestOp reqOp) {
        switch (reqOp.getRequestCase()) {
            case REQUEST_PUT:
                return buildPutOp(header, reqOp.getRequestPut());
            case REQUEST_DELETE_RANGE:
                return buildDeleteOp(header, reqOp.getRequestDeleteRange());
            case REQUEST_RANGE:
                return buildRangeOp(header, reqOp.getRequestRange());
            default:
                throw new IllegalArgumentException("unknown request type in a transaction" + reqOp.getRequestCase());
        }
    }


}
