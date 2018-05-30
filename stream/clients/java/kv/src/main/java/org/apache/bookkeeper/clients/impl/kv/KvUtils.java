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

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import org.apache.bookkeeper.api.kv.impl.result.IncrementResultImpl;
import org.apache.bookkeeper.api.kv.impl.result.KeyValueFactory;
import org.apache.bookkeeper.api.kv.impl.result.PutResultImpl;
import org.apache.bookkeeper.api.kv.impl.result.ResultFactory;
import org.apache.bookkeeper.api.kv.impl.result.TxnResultImpl;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.DeleteOp;
import org.apache.bookkeeper.api.kv.op.Op;
import org.apache.bookkeeper.api.kv.op.PutOp;
import org.apache.bookkeeper.api.kv.op.RangeOp;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.IncrementOption;
import org.apache.bookkeeper.api.kv.options.PutOption;
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.api.kv.result.IncrementResult;
import org.apache.bookkeeper.api.kv.result.PutResult;
import org.apache.bookkeeper.api.kv.result.RangeResult;
import org.apache.bookkeeper.api.kv.result.TxnResult;
import org.apache.bookkeeper.stream.proto.kv.KeyValue;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare.CompareResult;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare.CompareTarget;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.PutResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeResponse;
import org.apache.bookkeeper.stream.proto.kv.rpc.RequestOp;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnResponse;

/**
 * K/V related utils.
 */
public final class KvUtils {

    private KvUtils() {
    }

    public static ByteString toProtoKey(ByteBuf key) {
        return UnsafeByteOperations.unsafeWrap(key.nioBuffer());
    }

    public static org.apache.bookkeeper.api.kv.result.KeyValue<ByteBuf, ByteBuf> fromProtoKeyValue(
        KeyValue kv,
        KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
        return kvFactory.newKv()
            .key(Unpooled.wrappedBuffer(kv.getKey().asReadOnlyByteBuffer()))
            .value(Unpooled.wrappedBuffer(kv.getValue().asReadOnlyByteBuffer()))
            .isNumber(kv.getIsNumber())
            .numberValue(kv.getNumberValue())
            .createRevision(kv.getCreateRevision())
            .modifiedRevision(kv.getModRevision())
            .version(kv.getVersion());
    }

    public static List<org.apache.bookkeeper.api.kv.result.KeyValue<ByteBuf, ByteBuf>> fromProtoKeyValues(
        List<KeyValue> kvs, KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
        return Lists.transform(kvs, kv -> fromProtoKeyValue(kv, kvFactory));
    }

    public static RangeRequest.Builder newRangeRequest(ByteBuf key, RangeOption<ByteBuf> option) {
        RangeRequest.Builder builder = RangeRequest.newBuilder()
            .setKey(toProtoKey(key))
            .setCountOnly(option.countOnly())
            .setKeysOnly(option.keysOnly())
            .setLimit(option.limit())
            .setMinCreateRevision(option.minCreateRev())
            .setMaxCreateRevision(option.maxCreateRev())
            .setMinModRevision(option.minModRev())
            .setMaxModRevision(option.maxModRev());
        if (null != option.endKey()) {
            builder = builder.setRangeEnd(toProtoKey(option.endKey()));
        }
        return builder;
    }

    public static RangeResult<ByteBuf, ByteBuf> newRangeResult(
        RangeResponse response,
        ResultFactory<ByteBuf, ByteBuf> resultFactory,
        KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
        return resultFactory.newRangeResult(-1L)
            .count(response.getCount())
            .more(response.getMore())
            .kvs(fromProtoKeyValues(response.getKvsList(), kvFactory));
    }

    public static PutRequest.Builder newPutRequest(ByteBuf key,
                                                   ByteBuf value,
                                                   PutOption<ByteBuf> option) {
        return PutRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(key.nioBuffer()))
            .setValue(UnsafeByteOperations.unsafeWrap(value.nioBuffer()))
            .setPrevKv(option.prevKv());
    }

    public static PutResult<ByteBuf, ByteBuf> newPutResult(
        PutResponse response,
        ResultFactory<ByteBuf, ByteBuf> resultFactory,
        KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
        PutResultImpl<ByteBuf, ByteBuf> result = resultFactory.newPutResult(-1L);
        if (response.hasPrevKv()) {
            result.prevKv(fromProtoKeyValue(response.getPrevKv(), kvFactory));
        }
        return result;
    }

    public static IncrementRequest.Builder newIncrementRequest(ByteBuf key,
                                                               long amount,
                                                               IncrementOption<ByteBuf> option) {
        return IncrementRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(key.nioBuffer()))
            .setAmount(amount)
            .setGetTotal(option.getTotal());
    }

    public static IncrementResult<ByteBuf, ByteBuf> newIncrementResult(
        IncrementResponse response,
        ResultFactory<ByteBuf, ByteBuf> resultFactory,
        KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
        IncrementResultImpl<ByteBuf, ByteBuf> result = resultFactory.newIncrementResult(-1L)
            .totalAmount(response.getTotalAmount());
        return result;
    }

    public static DeleteRangeRequest.Builder newDeleteRequest(ByteBuf key, DeleteOption<ByteBuf> option) {
        DeleteRangeRequest.Builder builder = DeleteRangeRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(key.nioBuffer()))
            .setPrevKv(option.prevKv());
        if (null != option.endKey()) {
            builder = builder.setRangeEnd(UnsafeByteOperations.unsafeWrap(option.endKey().nioBuffer()));
        }
        return builder;
    }

    public static DeleteResult<ByteBuf, ByteBuf> newDeleteResult(
        DeleteRangeResponse response,
        ResultFactory<ByteBuf, ByteBuf> resultFactory,
        KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
        return resultFactory.newDeleteResult(-1L)
            .numDeleted(response.getDeleted())
            .prevKvs(fromProtoKeyValues(response.getPrevKvsList(), kvFactory));
    }

    public static CompareTarget toProtoTarget(org.apache.bookkeeper.api.kv.op.CompareTarget target) {
        switch (target) {
            case MOD:
                return CompareTarget.MOD;
            case VALUE:
                return CompareTarget.VALUE;
            case CREATE:
                return CompareTarget.CREATE;
            case VERSION:
                return CompareTarget.VERSION;
            default:
                return CompareTarget.UNRECOGNIZED;
        }
    }

    public static CompareResult toProtoResult(org.apache.bookkeeper.api.kv.op.CompareResult result) {
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
                return CompareResult.UNRECOGNIZED;
        }
    }

    public static Compare.Builder toProtoCompare(CompareOp<ByteBuf, ByteBuf> cmp) {
        Compare.Builder builder = Compare.newBuilder()
            .setTarget(toProtoTarget(cmp.target()))
            .setResult(toProtoResult(cmp.result()))
            .setKey(toProtoKey(cmp.key()));
        switch (cmp.target()) {
            case VERSION:
                builder.setVersion(cmp.revision());
                break;
            case MOD:
                builder.setModRevision(cmp.revision());
                break;
            case CREATE:
                builder.setCreateRevision(cmp.revision());
                break;
            case VALUE:
                ByteBuf value = cmp.value();
                if (null == value) {
                    value = Unpooled.wrappedBuffer(new byte[0]);
                }
                builder.setValue(toProtoKey(value));
                break;
            default:
                break;
        }
        return builder;
    }

    public static PutRequest.Builder toProtoPutRequest(PutOp<ByteBuf, ByteBuf> op) {
        return PutRequest.newBuilder()
            .setPrevKv(op.option().prevKv())
            .setKey(toProtoKey(op.key()))
            .setValue(toProtoKey(op.value()));
    }

    public static DeleteRangeRequest.Builder toProtoDeleteRequest(DeleteOp<ByteBuf, ByteBuf> op) {
        DeleteRangeRequest.Builder builder = DeleteRangeRequest.newBuilder()
            .setKey(toProtoKey(op.key()))
            .setPrevKv(op.option().prevKv());
        if (null != op.option().endKey()) {
            builder.setRangeEnd(toProtoKey(op.option().endKey()));
        }
        return builder;
    }

    public static RangeRequest.Builder toProtoRangeRequest(RangeOp<ByteBuf, ByteBuf> op) {
        RangeRequest.Builder builder = RangeRequest.newBuilder()
            .setKey(toProtoKey(op.key()))
            .setCountOnly(op.option().countOnly())
            .setKeysOnly(op.option().keysOnly())
            .setLimit(op.option().limit());
        if (null != op.option().endKey()) {
            builder.setRangeEnd(toProtoKey(op.option().endKey()));
        }
        return builder;
    }

    public static RequestOp.Builder toProtoRequest(Op<ByteBuf, ByteBuf> op) {
        RequestOp.Builder reqBuilder = RequestOp.newBuilder();
        switch (op.type()) {
            case DELETE:
                reqBuilder.setRequestDeleteRange(toProtoDeleteRequest((DeleteOp<ByteBuf, ByteBuf>) op));
                break;
            case RANGE:
                reqBuilder.setRequestRange(toProtoRangeRequest((RangeOp<ByteBuf, ByteBuf>) op));
                break;
            case PUT:
                reqBuilder.setRequestPut(toProtoPutRequest((PutOp<ByteBuf, ByteBuf>) op));
                break;
            default:
                throw new IllegalArgumentException("Type '" + op.type() + "' is not supported in a txn yet.");
        }
        return reqBuilder;
    }

    public static TxnResult<ByteBuf, ByteBuf> newKvTxnResult(
        TxnResponse txnResponse,
        ResultFactory<ByteBuf, ByteBuf> resultFactory,
        KeyValueFactory<ByteBuf, ByteBuf> kvFactory) {
        TxnResultImpl<ByteBuf, ByteBuf> result = resultFactory.newTxnResult(-1L);
        result.isSuccess(txnResponse.getSucceeded());
        result.results(Lists.transform(txnResponse.getResponsesList(), op -> {
            switch (op.getResponseCase()) {
                case RESPONSE_PUT:
                    return newPutResult(
                        op.getResponsePut(),
                        resultFactory,
                        kvFactory);
                case RESPONSE_RANGE:
                    return newRangeResult(
                        op.getResponseRange(),
                        resultFactory,
                        kvFactory);
                case RESPONSE_DELETE_RANGE:
                    return newDeleteResult(
                        op.getResponseDeleteRange(),
                        resultFactory,
                        kvFactory);
                default:
                    throw new IllegalArgumentException("Unknown response type '" + op.getResponseCase() + "'");
            }
        }));
        return result;
    }

}
