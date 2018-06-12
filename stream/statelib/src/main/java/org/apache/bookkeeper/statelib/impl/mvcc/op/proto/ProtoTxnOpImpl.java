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

package org.apache.bookkeeper.statelib.impl.mvcc.op.proto;

import static org.apache.bookkeeper.statelib.impl.mvcc.MVCCUtils.toApiOp;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.List;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.Op;
import org.apache.bookkeeper.api.kv.op.OpType;
import org.apache.bookkeeper.api.kv.op.TxnOp;
import org.apache.bookkeeper.common.collections.RecyclableArrayList;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare;
import org.apache.bookkeeper.stream.proto.kv.rpc.RequestOp;
import org.apache.bookkeeper.stream.proto.kv.rpc.TxnRequest;

/**
 * A protobuf encoded transaction operation.
 */
@RequiredArgsConstructor
@ToString(exclude = "recyclerHandle")
@Setter(AccessLevel.PRIVATE)
public class ProtoTxnOpImpl implements TxnOp<byte[], byte[]> {

    public static ProtoTxnOpImpl newTxnOp(TxnRequest request) {
        ProtoTxnOpImpl op = RECYCLER.get();
        op.setRequest(request);
        RecyclableArrayList<CompareOp<byte[], byte[]>> compareOps = COMPARE_OPS_RECYCLER.newInstance();
        for (Compare compare : request.getCompareList()) {
            compareOps.add(ProtoCompareImpl.newCompareOp(compare));
        }
        op.setCompareOps(compareOps);
        RecyclableArrayList<Op<byte[], byte[]>> successOps = OPS_RECYCLER.newInstance();
        for (RequestOp reqOp : request.getSuccessList()) {
            successOps.add(toApiOp(reqOp));
        }
        op.setSuccessOps(successOps);
        RecyclableArrayList<Op<byte[], byte[]>> failureOps = OPS_RECYCLER.newInstance();
        for (RequestOp reqOp : request.getFailureList()) {
            failureOps.add(toApiOp(reqOp));
        }
        return op;
    }

    private static final Recycler<ProtoTxnOpImpl> RECYCLER = new Recycler<ProtoTxnOpImpl>() {
        @Override
        protected ProtoTxnOpImpl newObject(Handle<ProtoTxnOpImpl> handle) {
            return new ProtoTxnOpImpl(handle);
        }
    };

    private static final RecyclableArrayList.Recycler<CompareOp<byte[], byte[]>> COMPARE_OPS_RECYCLER =
        new RecyclableArrayList.Recycler<>();
    private static final RecyclableArrayList.Recycler<Op<byte[], byte[]>> OPS_RECYCLER =
        new RecyclableArrayList.Recycler<>();

    private final Handle<ProtoTxnOpImpl> recyclerHandle;
    private TxnRequest request;
    private RecyclableArrayList<CompareOp<byte[], byte[]>> compareOps;
    private RecyclableArrayList<Op<byte[], byte[]>> successOps;
    private RecyclableArrayList<Op<byte[], byte[]>> failureOps;

    private void reset() {
        request = null;
        if (null != compareOps) {
            compareOps.forEach(CompareOp::close);
            compareOps.recycle();
        }
        if (null != successOps) {
            successOps.forEach(Op::close);
            successOps.recycle();
        }
        if (null != failureOps) {
            failureOps.forEach(Op::close);
            failureOps.recycle();
        }
    }

    @Override
    public List<CompareOp<byte[], byte[]>> compareOps() {
        return compareOps;
    }

    @Override
    public List<Op<byte[], byte[]>> successOps() {
        return successOps;
    }

    @Override
    public List<Op<byte[], byte[]>> failureOps() {
        return failureOps;
    }

    @Override
    public OpType type() {
        return OpType.TXN;
    }

    @Override
    public void close() {
        reset();
        recyclerHandle.recycle(this);
    }
}
