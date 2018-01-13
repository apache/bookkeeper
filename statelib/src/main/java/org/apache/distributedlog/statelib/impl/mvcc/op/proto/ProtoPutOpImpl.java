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
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.common.util.Recycled;
import org.apache.distributedlog.statelib.api.mvcc.op.OpType;
import org.apache.distributedlog.statelib.api.mvcc.op.PutOp;
import org.apache.distributedlog.statelib.impl.Constants;
import org.apache.distributedlog.statestore.proto.Command;
import org.apache.distributedlog.statestore.proto.PutRequest;

/**
 * A protobuf encoded put operation.
 */
@RequiredArgsConstructor
public class ProtoPutOpImpl implements PutOp<byte[], byte[]>, Recycled {

    public static ProtoPutOpImpl newPutOp(long revision, Command command) {
        ProtoPutOpImpl op = RECYCLER.get();
        op.setCommand(revision, command);
        return op;
    }

    private static final Recycler<ProtoPutOpImpl> RECYCLER = new Recycler<ProtoPutOpImpl>() {
        @Override
        protected ProtoPutOpImpl newObject(Handle<ProtoPutOpImpl> handle) {
            return new ProtoPutOpImpl(handle);
        }
    };

    private final Handle<ProtoPutOpImpl> recyclerHandle;
    private PutRequest req;
    private byte[] key;
    private byte[] value;
    private long revision;

    @Override
    public byte[] value() {
        if (null != value) {
            return value;
        }
        value = req.getValue().toByteArray();
        return value;
    }

    public void setCommand(long revision, Command command) {
        this.req = command.getPutReq();
        this.revision = revision;
    }

    @Override
    public boolean prevKV() {
        return req.getPrevKv();
    }

    @Override
    public OpType type() {
        return OpType.PUT;
    }

    @Override
    public byte[] key() {
        if (null != key) {
            return key;
        }
        key = req.getKey().toByteArray();
        return key;
    }

    @Override
    public long revision() {
        return revision;
    }

    void reset() {
        req = null;
        key = null;
        value = null;
        revision = Constants.INVALID_REVISION;
    }

    @Override
    public void recycle() {
        reset();
        recyclerHandle.recycle(this);
    }
}
