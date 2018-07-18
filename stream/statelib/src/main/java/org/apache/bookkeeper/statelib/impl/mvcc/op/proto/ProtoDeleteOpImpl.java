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
import org.apache.bookkeeper.api.kv.op.DeleteOp;
import org.apache.bookkeeper.api.kv.op.OpType;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.stream.proto.kv.rpc.DeleteRangeRequest;

/**
 * A protobuf encoded delete operation.
 */
@RequiredArgsConstructor
@ToString(exclude = "recyclerHandle")
public class ProtoDeleteOpImpl implements DeleteOp<byte[], byte[]>, DeleteOption<byte[]> {

    public static ProtoDeleteOpImpl newDeleteOp(DeleteRangeRequest req) {
        ProtoDeleteOpImpl op = RECYCLER.get();
        op.setCommand(req);
        return op;
    }

    private static final Recycler<ProtoDeleteOpImpl> RECYCLER = new Recycler<ProtoDeleteOpImpl>() {
        @Override
        protected ProtoDeleteOpImpl newObject(Handle<ProtoDeleteOpImpl> handle) {
            return new ProtoDeleteOpImpl(handle);
        }
    };

    private final Handle<ProtoDeleteOpImpl> recyclerHandle;
    private DeleteRangeRequest req;
    private byte[] key;
    private byte[] endKey;

    private void reset() {
        req = null;
        key = null;
        endKey = null;
    }

    private void setCommand(DeleteRangeRequest req) {
        this.req = req;
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
    public boolean prevKv() {
        return req.getPrevKv();
    }

    @Override
    public DeleteOption<byte[]> option() {
        return this;
    }

    @Override
    public void close() {
        reset();
        recyclerHandle.recycle(this);
    }

    @Override
    public OpType type() {
        return OpType.DELETE;
    }

}
