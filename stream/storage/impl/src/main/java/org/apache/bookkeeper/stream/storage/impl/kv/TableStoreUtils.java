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

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.util.concurrent.ExecutionException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.op.OpFactory;
import org.apache.bookkeeper.api.kv.result.Code;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.api.kv.result.IncrementResult;
import org.apache.bookkeeper.api.kv.result.PutResult;
import org.apache.bookkeeper.api.kv.result.RangeResult;
import org.apache.bookkeeper.api.kv.result.Result;
import org.apache.bookkeeper.api.kv.result.TxnResult;
import org.apache.bookkeeper.statelib.api.exceptions.MVCCStoreException;
import org.apache.bookkeeper.stream.proto.kv.KeyValue;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.ResponseHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.ResponseOp;
import org.apache.bookkeeper.stream.proto.kv.rpc.RoutingHeader;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;
import org.apache.bookkeeper.stream.proto.storage.StatusCode;

/**
 * Utils for accessing table stores.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class TableStoreUtils {

    static final byte SEP = 0x0;
    static final byte NO_ROUTING_KEY = 0x0;
    static final byte HAS_ROUTING_KEY = 0x1;

    static boolean hasRKey(ByteString rKey) {
        return null != rKey && rKey.size() > 0;
    }

    static byte[] newStoreKey(ByteString rKey, ByteString lKey) {
        boolean hasRkey = hasRKey(rKey);

        int keyLen;
        if (hasRkey) {
            keyLen = rKey.size() + lKey.size() + 2;
        } else {
            keyLen = lKey.size() + 1;
        }
        ByteBuf keyBuf = Unpooled.buffer(keyLen);
        if (hasRkey) {
            keyBuf.writeByte(HAS_ROUTING_KEY);
            keyBuf.writeBytes(rKey.asReadOnlyByteBuffer());
            keyBuf.writeByte(SEP);
            keyBuf.writeBytes(lKey.asReadOnlyByteBuffer());
        } else {
            keyBuf.writeByte(NO_ROUTING_KEY);
            keyBuf.writeBytes(lKey.asReadOnlyByteBuffer());
        }

        return ByteBufUtil.getBytes(keyBuf);
    }

    static ByteString getLKey(byte[] storeKey, ByteString rKey) {
        boolean hasRKey = hasRKey(rKey);

        int lKeyOffset;
        if (hasRKey) {
            lKeyOffset = rKey.size() + 2;
        } else {
            lKeyOffset = 1;
        }

        return UnsafeByteOperations.unsafeWrap(storeKey, lKeyOffset, storeKey.length - lKeyOffset);
    }

    static StatusCode handleCause(Throwable cause) {
        if (cause instanceof ExecutionException) {
            return handleCause(cause.getCause());
        } else if (cause instanceof MVCCStoreException) {
            MVCCStoreException mse = (MVCCStoreException) cause;
            return mvccCodeToStatusCode(mse.getCode());
        } else {
            return StatusCode.INTERNAL_SERVER_ERROR;
        }
    }

    static StatusCode mvccCodeToStatusCode(Code code) {
        switch (code) {
            case OK:
                return StatusCode.SUCCESS;
            case INTERNAL_ERROR:
                return StatusCode.INTERNAL_SERVER_ERROR;
            case INVALID_ARGUMENT:
                return StatusCode.BAD_REQUEST;
            case ILLEGAL_OP:
                return StatusCode.BAD_REQUEST;
            case UNEXPECTED:
                return StatusCode.UNEXPECTED;
            case BAD_REVISION:
                return StatusCode.BAD_REVISION;
            case SMALLER_REVISION:
                return StatusCode.BAD_REVISION;
            case KEY_NOT_FOUND:
                return StatusCode.KEY_NOT_FOUND;
            case KEY_EXISTS:
                return StatusCode.KEY_EXISTS;
            default:
                return StatusCode.INTERNAL_SERVER_ERROR;
        }
    }

    static KeyValue newKeyValue(ByteString rKey,
                                org.apache.bookkeeper.api.kv.result.KeyValue<byte[], byte[]> kv) {
        if (null == kv) {
            return null;
        }
        return KeyValue.newBuilder()
            .setKey(getLKey(kv.key(), rKey))
            .setValue(UnsafeByteOperations.unsafeWrap(kv.value()))
            .setCreateRevision(kv.createRevision())
            .setModRevision(kv.modifiedRevision())
            .setVersion(kv.version())
            .setIsNumber(kv.isNumber())
            .setNumberValue(kv.numberValue())
            .build();
    }

    static PutResponse processPutResult(RoutingHeader routingHeader,
                                        PutResult<byte[], byte[]> result) {
        ByteString rKey = routingHeader.getRKey();
        PutResponse.Builder putRespBuilder = PutResponse.newBuilder()
            .setHeader(ResponseHeader.newBuilder()
                .setCode(mvccCodeToStatusCode(result.code()))
                .setRoutingHeader(routingHeader)
                .build());
        if (null != result.prevKv()) {
            putRespBuilder = putRespBuilder.setPrevKv(newKeyValue(rKey, result.prevKv()));
        }
        return putRespBuilder.build();
    }

    static IncrementResponse processIncrementResult(RoutingHeader routingHeader,
                                                    IncrementResult<byte[], byte[]> result) {
        IncrementResponse.Builder putRespBuilder = IncrementResponse.newBuilder()
            .setHeader(ResponseHeader.newBuilder()
                .setCode(mvccCodeToStatusCode(result.code()))
                .setRoutingHeader(routingHeader)
                .build())
            .setTotalAmount(result.totalAmount());
        return putRespBuilder.build();
    }

    static RangeResponse processRangeResult(RoutingHeader routingHeader,
                                            RangeResult<byte[], byte[]> result) {
        ByteString rKey = routingHeader.getRKey();
        return RangeResponse.newBuilder()
            .setCount(result.count())
            .setHeader(ResponseHeader.newBuilder()
                .setCode(mvccCodeToStatusCode(result.code()))
                .setRoutingHeader(routingHeader)
                .build())
            .addAllKvs(Lists.transform(result.kvs(), kv -> newKeyValue(rKey, kv)))
            .setMore(result.more())
            .build();
    }

    static DeleteRangeResponse processDeleteResult(RoutingHeader routingHeader,
                                                   DeleteResult<byte[], byte[]> result) {
        ByteString rKey = routingHeader.getRKey();
        return DeleteRangeResponse.newBuilder()
            .setHeader(ResponseHeader.newBuilder()
                .setCode(mvccCodeToStatusCode(result.code()))
                .setRoutingHeader(routingHeader)
                .build())
            .setDeleted(result.numDeleted())
            .addAllPrevKvs(Lists.transform(result.prevKvs(), kv -> newKeyValue(rKey, kv)))
            .build();
    }

    static TxnResponse processTxnResult(RoutingHeader routingHeader,
                                        TxnResult<byte[], byte[]> txnResult) {
        return TxnResponse.newBuilder()
            .setHeader(ResponseHeader.newBuilder()
                .setCode(mvccCodeToStatusCode(txnResult.code()))
                .setRoutingHeader(routingHeader)
                .build())
            .setSucceeded(txnResult.isSuccess())
            .addAllResponses(Lists.transform(txnResult.results(), result -> processTxnResult(
                routingHeader, result)))
            .build();
    }

    static ResponseOp processTxnResult(RoutingHeader routingHeader,
                                       Result<byte[], byte[]> result) {

        ResponseOp.Builder respBuilder = ResponseOp.newBuilder();
        switch (result.type()) {
            case PUT:
                PutResult<byte[], byte[]> putResult = (PutResult<byte[], byte[]>) result;
                respBuilder.setResponsePut(
                    processPutResult(routingHeader, putResult));
                break;
            case DELETE:
                DeleteResult<byte[], byte[]> delResult = (DeleteResult<byte[], byte[]>) result;
                respBuilder.setResponseDeleteRange(
                    processDeleteResult(routingHeader, delResult));
                break;
            case RANGE:
                RangeResult<byte[], byte[]> rangeResult = (RangeResult<byte[], byte[]>) result;
                respBuilder.setResponseRange(
                    processRangeResult(routingHeader, rangeResult));
                break;
            default:
                break;
        }
        return respBuilder.build();
    }

    static CompareOp<byte[], byte[]> fromProtoCompare(OpFactory<byte[], byte[]> opFactory,
                                                      RoutingHeader header,
                                                      Compare compare) {
        ByteString rKey = header.getRKey();
        ByteString lKey = compare.getKey();
        byte[] storeKey = newStoreKey(rKey, lKey);
        CompareResult result = fromProtoCompareResult(compare.getResult());
        switch (compare.getTarget()) {
            case MOD:
                return opFactory.compareModRevision(
                    result,
                    storeKey,
                    compare.getModRevision());
            case CREATE:
                return opFactory.compareCreateRevision(
                    result,
                    storeKey,
                    compare.getCreateRevision());
            case VERSION:
                return opFactory.compareVersion(
                    result,
                    storeKey,
                    compare.getVersion());
            case VALUE:
                return opFactory.compareValue(
                    result,
                    storeKey,
                    (null == compare.getValue() || compare.getValue().size() == 0)
                        ? null : compare.getValue().toByteArray());
            default:
                throw new IllegalArgumentException("Invalid compare target " + compare.getTarget());
        }

    }

    static CompareResult fromProtoCompareResult(Compare.CompareResult result) {
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
                throw new IllegalArgumentException("Invalid compare result " + result);
        }
    }

}
