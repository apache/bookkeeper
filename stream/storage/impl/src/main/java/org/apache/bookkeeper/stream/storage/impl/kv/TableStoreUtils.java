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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import lombok.AccessLevel;
import lombok.CustomLog;
import lombok.NoArgsConstructor;
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
@CustomLog
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class TableStoreUtils {

    static final byte SEP = 0x0;
    static final byte NO_ROUTING_KEY = 0x0;
    static final byte HAS_ROUTING_KEY = 0x1;

    static boolean hasRKey(byte[] rKey) {
        return null != rKey && rKey.length > 0;
    }

    static byte[] newStoreKey(byte[] rKey, byte[] lKey) {
        boolean hasRkey = hasRKey(rKey);

        int keyLen;
        if (hasRkey) {
            keyLen = rKey.length + lKey.length + 2;
        } else {
            keyLen = lKey.length + 1;
        }
        ByteBuf keyBuf = Unpooled.buffer(keyLen);
        if (hasRkey) {
            keyBuf.writeByte(HAS_ROUTING_KEY);
            keyBuf.writeBytes(rKey);
            keyBuf.writeByte(SEP);
            keyBuf.writeBytes(lKey);
        } else {
            keyBuf.writeByte(NO_ROUTING_KEY);
            keyBuf.writeBytes(lKey);
        }

        return ByteBufUtil.getBytes(keyBuf);
    }

    static byte[] getLKey(byte[] storeKey, byte[] rKey) {
        boolean hasRKey = hasRKey(rKey);

        int lKeyOffset;
        if (hasRKey) {
            lKeyOffset = rKey.length + 2;
        } else {
            lKeyOffset = 1;
        }

        return Arrays.copyOfRange(storeKey, lKeyOffset, storeKey.length);
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

    static KeyValue newKeyValue(byte[] rKey,
                                org.apache.bookkeeper.api.kv.result.KeyValue<byte[], byte[]> kv) {
        if (null == kv) {
            return null;
        }
        return new KeyValue()
            .setKey(getLKey(kv.key(), rKey))
            .setValue(kv.value())
            .setCreateRevision(kv.createRevision())
            .setModRevision(kv.modifiedRevision())
            .setVersion(kv.version())
            .setIsNumber(kv.isNumber())
            .setNumberValue(kv.numberValue());
    }

    static PutResponse processPutResult(RoutingHeader routingHeader,
                                        PutResult<byte[], byte[]> result) {
        byte[] rKey = routingHeader.getRKey();
        PutResponse putResp = new PutResponse();
        ResponseHeader header = putResp.setHeader();
        header.setCode(mvccCodeToStatusCode(result.code()));
        header.setRoutingHeader().copyFrom(routingHeader);
        if (null != result.prevKv()) {
            putResp.setPrevKv().copyFrom(newKeyValue(rKey, result.prevKv()));
        }
        return putResp;
    }

    static IncrementResponse processIncrementResult(RoutingHeader routingHeader,
                                                    IncrementResult<byte[], byte[]> result) {
        IncrementResponse putResp = new IncrementResponse();
        ResponseHeader header = putResp.setHeader();
        header.setCode(mvccCodeToStatusCode(result.code()));
        header.setRoutingHeader().copyFrom(routingHeader);
        putResp.setTotalAmount(result.totalAmount());
        return putResp;
    }

    static RangeResponse processRangeResult(RoutingHeader routingHeader,
                                            RangeResult<byte[], byte[]> result) {
        byte[] rKey = routingHeader.getRKey();
        RangeResponse rangeResp = new RangeResponse();
        rangeResp.setCount(result.count());
        ResponseHeader header = rangeResp.setHeader();
        header.setCode(mvccCodeToStatusCode(result.code()));
        header.setRoutingHeader().copyFrom(routingHeader);
        for (org.apache.bookkeeper.api.kv.result.KeyValue<byte[], byte[]> kv : result.kvs()) {
            rangeResp.addKv().copyFrom(newKeyValue(rKey, kv));
        }
        rangeResp.setMore(result.more());
        return rangeResp;
    }

    static DeleteRangeResponse processDeleteResult(RoutingHeader routingHeader,
                                                   DeleteResult<byte[], byte[]> result) {
        byte[] rKey = routingHeader.getRKey();
        DeleteRangeResponse delResp = new DeleteRangeResponse();
        ResponseHeader header = delResp.setHeader();
        header.setCode(mvccCodeToStatusCode(result.code()));
        header.setRoutingHeader().copyFrom(routingHeader);
        delResp.setDeleted(result.numDeleted());
        for (org.apache.bookkeeper.api.kv.result.KeyValue<byte[], byte[]> kv : result.prevKvs()) {
            delResp.addPrevKv().copyFrom(newKeyValue(rKey, kv));
        }
        return delResp;
    }

    static TxnResponse processTxnResult(RoutingHeader routingHeader,
                                        TxnResult<byte[], byte[]> txnResult) {
        TxnResponse txnResp = new TxnResponse();
        ResponseHeader header = txnResp.setHeader();
        header.setCode(mvccCodeToStatusCode(txnResult.code()));
        header.setRoutingHeader().copyFrom(routingHeader);
        txnResp.setSucceeded(txnResult.isSuccess());
        for (Result<byte[], byte[]> result : txnResult.results()) {
            txnResp.addResponse().copyFrom(processTxnResult(routingHeader, result));
        }
        return txnResp;
    }

    static ResponseOp processTxnResult(RoutingHeader routingHeader,
                                       Result<byte[], byte[]> result) {

        ResponseOp respOp = new ResponseOp();
        switch (result.type()) {
            case PUT:
                PutResult<byte[], byte[]> putResult = (PutResult<byte[], byte[]>) result;
                respOp.setResponsePut().copyFrom(processPutResult(routingHeader, putResult));
                break;
            case DELETE:
                DeleteResult<byte[], byte[]> delResult = (DeleteResult<byte[], byte[]>) result;
                respOp.setResponseDeleteRange().copyFrom(processDeleteResult(routingHeader, delResult));
                break;
            case RANGE:
                RangeResult<byte[], byte[]> rangeResult = (RangeResult<byte[], byte[]>) result;
                respOp.setResponseRange().copyFrom(processRangeResult(routingHeader, rangeResult));
                break;
            default:
                break;
        }
        return respOp;
    }

    static CompareOp<byte[], byte[]> fromProtoCompare(OpFactory<byte[], byte[]> opFactory,
                                                      RoutingHeader header,
                                                      Compare compare) {
        byte[] rKey = header.getRKey();
        byte[] lKey = compare.getKey();
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
                byte[] cv = compare.getValue();
                return opFactory.compareValue(
                    result,
                    storeKey,
                    (null == cv || cv.length == 0)
                        ? null : cv);
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
