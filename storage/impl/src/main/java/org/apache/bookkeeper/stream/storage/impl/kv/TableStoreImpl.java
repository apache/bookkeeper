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
import static org.apache.bookkeeper.stream.storage.impl.kv.TableStoreUtils.mvccCodeToStatusCode;
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
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RequestOp;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerRequest;
import org.apache.bookkeeper.stream.proto.storage.StorageContainerResponse;
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
    public CompletableFuture<StorageContainerResponse> range(StorageContainerRequest request) {
        RangeRequest rangeReq = request.getKvRangeReq();

        if (log.isTraceEnabled()) {
            log.trace("Received range request {}", rangeReq);
        }
        return range(rangeReq)
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
            .thenApply(rangeResp -> StorageContainerResponse.newBuilder()
                .setCode(StatusCode.SUCCESS)
                .setKvRangeResp(rangeResp)
                .build())
            .exceptionally(cause -> {
                log.error("Failed to process range request {}", rangeReq, cause);
                return StorageContainerResponse.newBuilder()
                    .setCode(handleCause(cause))
                    .build();
            });
    }

    private CompletableFuture<RangeResult<byte[], byte[]>> range(RangeRequest request) {
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
    public CompletableFuture<StorageContainerResponse> put(StorageContainerRequest request) {
        PutRequest putReq = request.getKvPutReq();

        return put(putReq)
            .thenApply(result -> {
                try {
                    return processPutResult(
                        putReq.getHeader(),
                        result);
                } finally {
                    result.close();
                }
            })
            .thenApply(putResp -> StorageContainerResponse.newBuilder()
                .setCode(StatusCode.SUCCESS)
                .setKvPutResp(putResp)
                .build())
            .exceptionally(cause -> {
                log.error("Failed to process put request {}", putReq, cause);
                return StorageContainerResponse.newBuilder()
                    .setCode(handleCause(cause))
                    .build();
            });
    }

    private CompletableFuture<PutResult<byte[], byte[]>> put(PutRequest request) {
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
    public CompletableFuture<StorageContainerResponse> incr(StorageContainerRequest request) {
        IncrementRequest incrementReq = request.getKvIncrReq();

        return increment(incrementReq)
            .thenApply(result -> {
                try {
                    return processIncrementResult(
                        incrementReq.getHeader(),
                        result);
                } finally {
                    result.close();
                }
            })
            .thenApply(incrementResp -> StorageContainerResponse.newBuilder()
                .setCode(StatusCode.SUCCESS)
                .setKvIncrResp(incrementResp)
                .build())
            .exceptionally(cause -> {
                log.error("Failed to process increment request {}", incrementReq, cause);
                return StorageContainerResponse.newBuilder()
                    .setCode(handleCause(cause))
                    .build();
            });
    }

    private CompletableFuture<IncrementResult<byte[], byte[]>> increment(IncrementRequest request) {
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
            request.getAmount());
    }

    @Override
    public CompletableFuture<StorageContainerResponse> delete(StorageContainerRequest request) {
        DeleteRangeRequest deleteReq = request.getKvDeleteReq();

        return delete(deleteReq)
            .thenApply(result -> {
                try {
                    return processDeleteResult(
                        deleteReq.getHeader(),
                        result);
                } finally {
                    result.close();
                }
            })
            .thenApply(deleteResp -> StorageContainerResponse.newBuilder()
                .setCode(StatusCode.SUCCESS)
                .setKvDeleteResp(deleteResp)
                .build())
            .exceptionally(cause -> StorageContainerResponse.newBuilder()
                .setCode(handleCause(cause))
                .build());
    }

    private CompletableFuture<DeleteResult<byte[], byte[]>> delete(DeleteRangeRequest request) {
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
    public CompletableFuture<StorageContainerResponse> txn(StorageContainerRequest request) {
        TxnRequest txnReq = request.getKvTxnReq();

        if (log.isTraceEnabled()) {
            log.trace("Received txn request : {}", txnReq);
        }
        return txn(txnReq)
            .thenApply(txnResult -> {
                try {
                    TxnResponse txnResponse = processTxnResult(txnReq.getHeader(), txnResult);
                    return StorageContainerResponse.newBuilder()
                        .setCode(mvccCodeToStatusCode(txnResult.code()))
                        .setKvTxnResp(txnResponse)
                        .build();
                } finally {
                    txnResult.close();
                }
            })
            .exceptionally(cause -> StorageContainerResponse.newBuilder()
                .setCode(handleCause(cause))
                .build());
    }

    private CompletableFuture<TxnResult<byte[], byte[]>> txn(TxnRequest request) {
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
