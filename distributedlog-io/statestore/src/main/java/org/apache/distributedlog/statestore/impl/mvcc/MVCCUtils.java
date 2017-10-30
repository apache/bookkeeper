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

package org.apache.distributedlog.statestore.impl.mvcc;

import com.google.common.collect.Lists;
import com.google.protobuf.UnsafeByteOperations;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.distributedlog.statestore.api.mvcc.op.CompareOp;
import org.apache.distributedlog.statestore.api.mvcc.op.CompareResult;
import org.apache.distributedlog.statestore.api.mvcc.op.CompareTarget;
import org.apache.distributedlog.statestore.api.mvcc.op.DeleteOp;
import org.apache.distributedlog.statestore.api.mvcc.op.Op;
import org.apache.distributedlog.statestore.api.mvcc.op.PutOp;
import org.apache.distributedlog.statestore.api.mvcc.op.TxnOp;
import org.apache.distributedlog.statestore.proto.Compare;
import org.apache.distributedlog.statestore.proto.DeleteRequest;
import org.apache.distributedlog.statestore.proto.PutRequest;
import org.apache.distributedlog.statestore.proto.RequestOp;
import org.apache.distributedlog.statestore.proto.TxnRequest;

/**
 * Utils for mvcc stores.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class MVCCUtils {

    static PutRequest toPutRequest(PutOp<byte[], byte[]> op) {
        return PutRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(op.key()))
            .setValue(UnsafeByteOperations.unsafeWrap(op.value()))
            .setLease(0)
            .setPrevKv(op.prevKV())
            .build();

    }

    static DeleteRequest toDeleteRequest(DeleteOp<byte[], byte[]> op) {
        DeleteRequest.Builder reqBuilder = DeleteRequest.newBuilder()
            .setKey(UnsafeByteOperations.unsafeWrap(op.key()));
        if (null != op.endKey()) {
            reqBuilder = reqBuilder.setRangeEnd(
                UnsafeByteOperations.unsafeWrap(op.endKey()));
        }
        return reqBuilder.setPrevKv(op.prevKV()).build();
    }

    private static List<RequestOp> toRequestOpList(List<Op<byte[], byte[]>> ops) {
        List<RequestOp> requestOps = Lists.newArrayListWithExpectedSize(ops.size());
        for (Op<byte[], byte[]> op : ops) {
            switch (op.type()) {
                case PUT:
                    requestOps.add(RequestOp.newBuilder()
                        .setPutOp(toPutRequest((PutOp<byte[], byte[]>) op))
                        .build());
                    break;
                case DELETE:
                    requestOps.add(RequestOp.newBuilder()
                        .setDeleteOp(toDeleteRequest((DeleteOp<byte[], byte[]>) op))
                        .build());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown request "
                        + op.type() + " found in a txn request");
            }
        }
        return requestOps;
    }

    private static List<Compare> toCompareList(List<CompareOp<byte[], byte[]>> ops) {
        List<Compare> compares = Lists.newArrayListWithExpectedSize(ops.size());
        for (CompareOp op : ops) {
            compares.add(toCompare(op));
        }
        return compares;
    }

    private static Compare toCompare(CompareOp<byte[], byte[]> op) {
        Compare.Builder compareBuilder = Compare.newBuilder();
        compareBuilder.setTarget(toProtoCompareTarget(op.getTarget()));
        compareBuilder.setResult(toProtoCompareResult(op.getResult()));
        compareBuilder.setKey(UnsafeByteOperations.unsafeWrap(op.getKey()));
        switch (op.getTarget()) {
            case MOD:
                compareBuilder.setModRevision(op.getRevision());
                break;
            case CREATE:
                compareBuilder.setCreateRevision(op.getRevision());
                break;
            case VERSION:
                compareBuilder.setVersion(op.getRevision());
                break;
            case VALUE:
                compareBuilder.setValue(UnsafeByteOperations.unsafeWrap(op.getValue()));
                break;
            default:
                throw new IllegalArgumentException("Invalid compare target " + op.getTarget());
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

    static TxnRequest toTxnRequest(TxnOp<byte[], byte[]> op) {
        return TxnRequest.newBuilder()
            .addAllSuccess(toRequestOpList(op.successOps()))
            .addAllFailure(toRequestOpList(op.failureOps()))
            .addAllCompare(toCompareList(op.compareOps()))
            .build();
    }

}
