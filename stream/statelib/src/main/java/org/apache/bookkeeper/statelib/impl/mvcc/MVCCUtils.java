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

package org.apache.bookkeeper.statelib.impl.mvcc;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.op.CompareTarget;
import org.apache.bookkeeper.api.kv.op.DeleteOp;
import org.apache.bookkeeper.api.kv.op.IncrementOp;
import org.apache.bookkeeper.api.kv.op.Op;
import org.apache.bookkeeper.api.kv.op.PutOp;
import org.apache.bookkeeper.api.kv.op.RangeOp;
import org.apache.bookkeeper.api.kv.op.TxnOp;
import org.apache.bookkeeper.api.kv.result.Code;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.statelib.api.exceptions.MVCCStoreException;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreRuntimeException;
import org.apache.bookkeeper.statelib.impl.Constants;
import org.apache.bookkeeper.statelib.impl.mvcc.op.proto.ProtoDeleteOpImpl;
import org.apache.bookkeeper.statelib.impl.mvcc.op.proto.ProtoPutOpImpl;
import org.apache.bookkeeper.statelib.impl.mvcc.op.proto.ProtoRangeOpImpl;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RequestOp;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;
import org.apache.bookkeeper.stream.proto.kv.store.Command;
import org.apache.bookkeeper.stream.proto.kv.store.NopRequest;

/**
 * Utils for mvcc stores.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MVCCUtils {

    public static final Command NOP_CMD = Command.newBuilder()
        .setNopReq(NopRequest.newBuilder().build())
        .build();

    static PutRequest toPutRequest(PutOp<byte[], byte[]> op) {
        PutRequest.Builder reqBuilder = PutRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(op.key()))
            .setValue(UnsafeByteOperations.unsafeWrap(op.value()))
            .setLease(0)
            .setPrevKv(op.option().prevKv());
        return reqBuilder.build();
    }

    static DeleteRangeRequest toDeleteRequest(DeleteOp<byte[], byte[]> op) {
        byte[] key = op.key();
        if (null == key) {
            key = Constants.NULL_START_KEY;
        }
        byte[] endKey = op.option().endKey();
        if (null == endKey) {
            endKey = Constants.NULL_END_KEY;
        }

        DeleteRangeRequest.Builder reqBuilder = DeleteRangeRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(key))
            .setRangeEnd(UnsafeByteOperations.unsafeWrap(endKey));

        return reqBuilder.setPrevKv(op.option().prevKv()).build();
    }

    static RangeRequest toRangeRequest(RangeOp<byte[], byte[]> op) {
        byte[] key = op.key();
        if (null == key) {
            key = Constants.NULL_START_KEY;
        }
        byte[] endKey = op.option().endKey();
        if (null == endKey) {
            endKey = Constants.NULL_END_KEY;
        }

        RangeRequest.Builder reqBuilder = RangeRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(key))
            .setRangeEnd(UnsafeByteOperations.unsafeWrap(endKey))
            .setMaxCreateRevision(op.option().maxCreateRev())
            .setMinCreateRevision(op.option().minCreateRev())
            .setMaxModRevision(op.option().maxModRev())
            .setMinModRevision(op.option().minModRev())
            .setCountOnly(false);

        return reqBuilder.build();
    }

    private static List<RequestOp> toRequestOpList(List<Op<byte[], byte[]>> ops) {
        if (ops == null) {
            return Collections.emptyList();
        }
        List<RequestOp> requestOps = Lists.newArrayListWithExpectedSize(ops.size());
        for (Op<byte[], byte[]> op : ops) {
            switch (op.type()) {
                case PUT:
                    requestOps.add(RequestOp.newBuilder()
                        .setRequestPut(toPutRequest((PutOp<byte[], byte[]>) op))
                        .build());
                    break;
                case DELETE:
                    requestOps.add(RequestOp.newBuilder()
                        .setRequestDeleteRange(toDeleteRequest((DeleteOp<byte[], byte[]>) op))
                        .build());
                    break;
                case RANGE:
                    requestOps.add(RequestOp.newBuilder()
                        .setRequestRange(toRangeRequest((RangeOp<byte[], byte[]>) op))
                        .build());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown request "
                        + op.type() + " found in a txn request");
            }
        }
        return requestOps;
    }

    public static Op<byte[], byte[]> toApiOp(RequestOp protoOp) {
        switch (protoOp.getRequestCase()) {
            case REQUEST_PUT:
                return ProtoPutOpImpl.newPutOp(protoOp.getRequestPut());
            case REQUEST_RANGE:
                return ProtoRangeOpImpl.newRangeOp(protoOp.getRequestRange());
            case REQUEST_DELETE_RANGE:
                return ProtoDeleteOpImpl.newDeleteOp(protoOp.getRequestDeleteRange());
            default:
                throw new IllegalArgumentException("Unknown request "
                    + protoOp.getRequestCase() + " found in a txn request");
        }
    }

    private static List<Compare> toCompareList(List<CompareOp<byte[], byte[]>> ops) {
        List<Compare> compares = Lists.newArrayListWithExpectedSize(ops.size());
        for (CompareOp<byte[], byte[]> op : ops) {
            compares.add(toCompare(op));
        }
        return compares;
    }

    private static Compare toCompare(CompareOp<byte[], byte[]> op) {
        Compare.Builder compareBuilder = Compare.newBuilder();
        compareBuilder.setTarget(toProtoCompareTarget(op.target()));
        compareBuilder.setResult(toProtoCompareResult(op.result()));
        compareBuilder.setKey(UnsafeByteOperations.unsafeWrap(op.key()));
        switch (op.target()) {
            case MOD:
                compareBuilder.setModRevision(op.revision());
                break;
            case CREATE:
                compareBuilder.setCreateRevision(op.revision());
                break;
            case VERSION:
                compareBuilder.setVersion(op.revision());
                break;
            case VALUE:
                if (op.value() != null) {
                    compareBuilder.setValue(UnsafeByteOperations.unsafeWrap(op.value()));
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid compare target " + op.target());
        }
        return compareBuilder.build();
    }

    private static Compare.CompareTarget toProtoCompareTarget(CompareTarget target) {
        switch (target) {
            case MOD:
                return Compare.CompareTarget.MOD;
            case CREATE:
                return Compare.CompareTarget.CREATE;
            case VERSION:
                return Compare.CompareTarget.VERSION;
            case VALUE:
                return Compare.CompareTarget.VALUE;
            default:
                throw new IllegalArgumentException("Invalid compare target " + target);
        }
    }

    public static CompareTarget toApiCompareTarget(Compare.CompareTarget target) {
        switch (target) {
            case MOD:
                return CompareTarget.MOD;
            case CREATE:
                return CompareTarget.CREATE;
            case VERSION:
                return CompareTarget.VERSION;
            case VALUE:
                return CompareTarget.VALUE;
            default:
                throw new IllegalArgumentException("Invalid proto compare target " + target);
        }
    }

    private static Compare.CompareResult toProtoCompareResult(CompareResult result) {
        switch (result) {
            case LESS:
                return Compare.CompareResult.LESS;
            case EQUAL:
                return Compare.CompareResult.EQUAL;
            case GREATER:
                return Compare.CompareResult.GREATER;
            case NOT_EQUAL:
                return Compare.CompareResult.NOT_EQUAL;
            default:
                throw new IllegalArgumentException("Invalid compare result " + result);
        }
    }

    public static CompareResult toApiCompareResult(Compare.CompareResult result) {
        switch (result) {
            case LESS:
                return CompareResult.LESS;
            case EQUAL:
                return CompareResult.EQUAL;
            case GREATER:
                return CompareResult.GREATER;
            case NOT_EQUAL:
                return CompareResult.NOT_EQUAL;
            default:
                throw new IllegalArgumentException("Invalid proto compare result " + result);
        }
    }

    static TxnRequest toTxnRequest(TxnOp<byte[], byte[]> op) {
        return TxnRequest.newBuilder()
            .addAllSuccess(toRequestOpList(op.successOps()))
            .addAllFailure(toRequestOpList(op.failureOps()))
            .addAllCompare(toCompareList(op.compareOps()))
            .build();
    }

    static IncrementRequest toIncrementRequest(IncrementOp<byte[], byte[]> op) {
        return IncrementRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(op.key()))
            .setAmount(op.amount())
            .setGetTotal(op.option().getTotal())
            .build();
    }

    static Command toCommand(Op<byte[], byte[]> op) {
        Command.Builder cmdBuilder = Command.newBuilder();
        switch (op.type()) {
            case PUT:
                cmdBuilder.setPutReq(toPutRequest((PutOp<byte[], byte[]>) op));
                break;
            case DELETE:
                cmdBuilder.setDeleteReq(toDeleteRequest((DeleteOp<byte[], byte[]>) op));
                break;
            case TXN:
                cmdBuilder.setTxnReq(toTxnRequest((TxnOp<byte[], byte[]>) op));
                break;
            case INCREMENT:
                cmdBuilder.setIncrReq(toIncrementRequest((IncrementOp<byte[], byte[]>) op));
                break;
            default:
                throw new IllegalArgumentException("Unknown command type " + op.type());
        }
        return cmdBuilder.build();
    }

    public static ByteBuf newLogRecordBuf(Command command) {
        ByteBuf buf = Unpooled.buffer(command.getSerializedSize());
        try {
            command.writeTo(new ByteBufOutputStream(buf));
        } catch (IOException e) {
            throw new StateStoreRuntimeException("Invalid command : " + command, e);
        }
        return buf;
    }

    static Command newCommand(ByteBuf recordBuf) {
        try {
            return Command.parseFrom(recordBuf.nioBuffer());
        } catch (InvalidProtocolBufferException e) {
            log.error("Found a corrupted record on replaying log stream", e);
            throw new StateStoreRuntimeException("Found a corrupted record on replaying log stream", e);
        }
    }

    public static <T> CompletableFuture<T> failWithCode(Code code, String msg) {
        return FutureUtils.exception(new MVCCStoreException(code, msg));
    }

}
