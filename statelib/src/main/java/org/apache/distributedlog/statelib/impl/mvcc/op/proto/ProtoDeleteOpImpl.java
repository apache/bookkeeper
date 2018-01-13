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

package org.apache.distributedlog.statelib.impl.mvcc.op.proto;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.common.util.Recycled;
import org.apache.distributedlog.statelib.api.mvcc.op.DeleteOp;
import org.apache.distributedlog.statelib.api.mvcc.op.OpType;
import org.apache.distributedlog.statelib.impl.Constants;
import org.apache.distributedlog.statestore.proto.DeleteRequest;

/**
 * A protobuf encoded delete operation.
 */
@RequiredArgsConstructor
public class ProtoDeleteOpImpl implements DeleteOp<byte[], byte[]>, Recycled {

    public static ProtoDeleteOpImpl newDeleteOp(long revision, DeleteRequest req) {
        ProtoDeleteOpImpl op = RECYCLER.get();
        op.setCommand(revision, req);
        return op;
    }

    private static final Recycler<ProtoDeleteOpImpl> RECYCLER = new Recycler<ProtoDeleteOpImpl>() {
        @Override
        protected ProtoDeleteOpImpl newObject(Handle<ProtoDeleteOpImpl> handle) {
            return new ProtoDeleteOpImpl(handle);
        }
    };

    private final Handle<ProtoDeleteOpImpl> recyclerHandle;
    private DeleteRequest req;
    private Optional<byte[]> key;
    private Optional<byte[]> endKey;
    private long revision;

    private void reset() {
        req = null;
        key = null;
        endKey = null;
        revision = Constants.INVALID_REVISION;
    }

    private void setCommand(long revision, DeleteRequest req) {
        this.req = req;
        this.revision = revision;
    }

    @Override
    public void recycle() {
        reset();
        recyclerHandle.recycle(this);
    }

    @Override
    public Optional<byte[]> key() {
        if (null != key) {
            return key;
        }
        if (null == req.getKey()) {
            key = Optional.empty();
        } else {
            key = Optional.of(req.getKey().toByteArray());
        }
        return key;
    }

    @Override
    public Optional<byte[]> endKey() {
        if (null != endKey) {
            return endKey;
        }
        if (null == req.getRangeEnd()) {
            key = Optional.empty();
        } else {
            key = Optional.of(req.getKey().toByteArray());
        }
        return key;
    }

    @Override
    public boolean isRangeOp() {
        return endKey.isPresent();
    }

    @Override
    public boolean prevKV() {
        return req.getPrevKv();
    }

    @Override
    public OpType type() {
        return OpType.DELETE;
    }

    @Override
    public long revision() {
        return revision;
    }
}
