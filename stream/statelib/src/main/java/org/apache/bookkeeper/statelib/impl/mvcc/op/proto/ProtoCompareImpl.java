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

import static org.apache.bookkeeper.statelib.impl.Constants.INVALID_REVISION;
import static org.apache.bookkeeper.statelib.impl.mvcc.MVCCUtils.toApiCompareResult;
import static org.apache.bookkeeper.statelib.impl.mvcc.MVCCUtils.toApiCompareTarget;

import com.google.protobuf.ByteString;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.op.CompareTarget;
import org.apache.bookkeeper.stream.proto.kv.rpc.Compare;

/**
 * A protobuf encoded compare operation.
 */
@RequiredArgsConstructor
@Setter(AccessLevel.PRIVATE)
@ToString(exclude = "recyclerHandle")
public class ProtoCompareImpl implements CompareOp<byte[], byte[]> {

    /**
     * Create a protobuf encoded compare operation.
     *
     * @param protoCompare the protobuf representation of a compare operation.
     * @return a protobuf encoded compare operation
     */
    public static ProtoCompareImpl newCompareOp(Compare protoCompare) {
        ProtoCompareImpl op = RECYCLER.get();
        op.setRequest(protoCompare);
        op.setTarget(toApiCompareTarget(protoCompare.getTarget()));
        op.setResult(toApiCompareResult(protoCompare.getResult()));
        return op;
    }

    private static final Recycler<ProtoCompareImpl> RECYCLER = new Recycler<ProtoCompareImpl>() {
        @Override
        protected ProtoCompareImpl newObject(Handle<ProtoCompareImpl> handle) {
            return new ProtoCompareImpl(handle);
        }
    };

    private final Handle<ProtoCompareImpl> recyclerHandle;
    private Compare request;
    private CompareTarget target;
    private CompareResult result;
    private byte[] key;
    private byte[] value;

    private void reset() {
        request = null;
        key = null;
        value = null;
        target = null;
        result = null;
    }

    @Override
    public CompareTarget target() {
        return target;
    }

    @Override
    public CompareResult result() {
        return result;
    }

    @Override
    public byte[] key() {
        if (null != key) {
            return key;
        }
        if (ByteString.EMPTY == request.getKey()) {
            key = null;
        } else {
            key = request.getKey().toByteArray();
        }
        return key;
    }

    @Override
    public byte[] value() {
        if (null != value) {
            return value;
        }
        if (ByteString.EMPTY == request.getValue()) {
            value = null;
        } else {
            value = request.getValue().toByteArray();
        }
        return value;
    }

    @Override
    public long revision() {
        Compare req = request;
        if (null == req) {
            return INVALID_REVISION;
        } else {
            switch (req.getTarget()) {
                case MOD:
                    return req.getModRevision();
                case CREATE:
                    return req.getCreateRevision();
                case VERSION:
                    return req.getVersion();
                default:
                    return INVALID_REVISION;
            }
        }
    }

    @Override
    public void close() {
        reset();
        recyclerHandle.recycle(this);
    }
}
