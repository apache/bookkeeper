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

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.bookkeeper.api.kv.op.OpType;
import org.apache.bookkeeper.api.kv.op.RangeOp;
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.apache.bookkeeper.stream.proto.kv.rpc.RangeRequest;

/**
 * A protobuf encoded range operation.
 */
@RequiredArgsConstructor
@ToString(exclude = "recyclerHandle")
public class ProtoRangeOpImpl implements RangeOp<byte[], byte[]>, RangeOption<byte[]> {

    public static ProtoRangeOpImpl newRangeOp(RangeRequest request) {
        ProtoRangeOpImpl op = RECYCLER.get();
        op.setCommand(request);
        return op;
    }

    private static final Recycler<ProtoRangeOpImpl> RECYCLER = new Recycler<ProtoRangeOpImpl>() {
        @Override
        protected ProtoRangeOpImpl newObject(Handle<ProtoRangeOpImpl> handle) {
            return new ProtoRangeOpImpl(handle);
        }
    };

    private final Handle<ProtoRangeOpImpl> recyclerHandle;
    private RangeRequest req;
    private byte[] key;
    private byte[] endKey;

    void reset() {
        req = null;
        key = null;
        endKey = null;
    }

    void setCommand(RangeRequest request) {
        this.req = request;
    }

    @Override
    public void close() {
        reset();
        recyclerHandle.recycle(this);
    }

    @Override
    public byte[] key() {
        if (null != key) {
            return key;
        }
        if (null == req.getKey()) {
            key = null;
        } else {
            key = req.getKey().toByteArray();
        }
        return key;
    }

    @Override
    public RangeOption<byte[]> option() {
        return this;
    }

    @Override
    public byte[] endKey() {
        if (null != endKey) {
            return endKey;
        }
        if (null == req.getRangeEnd()
            || 0 == req.getRangeEnd().size()
            || (1 == req.getRangeEnd().size() && req.getRangeEnd().byteAt(0) == 0)) {
            endKey = null;
        } else {
            endKey = req.getRangeEnd().toByteArray();
        }
        return endKey;
    }

    @Override
    public long limit() {
        return req.getLimit();
    }

    @Override
    public long minModRev() {
        return req.getMinModRevision();
    }

    @Override
    public long maxModRev() {
        return req.getMaxModRevision();
    }

    @Override
    public long minCreateRev() {
        return req.getMinCreateRevision();
    }

    @Override
    public long maxCreateRev() {
        return req.getMaxCreateRevision();
    }

    @Override
    public boolean keysOnly() {
        // TODO: fix this
        return false;
    }

    @Override
    public boolean countOnly() {
        return req.getCountOnly();
    }

    @Override
    public OpType type() {
        return OpType.RANGE;
    }

}
