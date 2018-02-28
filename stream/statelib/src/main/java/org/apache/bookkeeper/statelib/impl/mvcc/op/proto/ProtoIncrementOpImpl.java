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
import org.apache.bookkeeper.api.kv.op.IncrementOp;
import org.apache.bookkeeper.api.kv.op.OpType;
import org.apache.bookkeeper.api.kv.options.IncrementOption;
import org.apache.bookkeeper.stream.proto.kv.rpc.IncrementRequest;
import org.apache.bookkeeper.stream.proto.kv.store.Command;

/**
 * A protobuf encoded increment operation.
 */
@RequiredArgsConstructor
public class ProtoIncrementOpImpl implements IncrementOp<byte[], byte[]>, IncrementOption<byte[]> {

    public static ProtoIncrementOpImpl newIncrementOp(Command command) {
        ProtoIncrementOpImpl op = RECYCLER.get();
        op.setCommand(command);
        return op;
    }

    private static final Recycler<ProtoIncrementOpImpl> RECYCLER = new Recycler<ProtoIncrementOpImpl>() {
        @Override
        protected ProtoIncrementOpImpl newObject(Handle<ProtoIncrementOpImpl> handle) {
            return new ProtoIncrementOpImpl(handle);
        }
    };

    private final Handle<ProtoIncrementOpImpl> recyclerHandle;
    private IncrementRequest req;
    private byte[] key;

    @Override
    public long amount() {
        return req.getAmount();
    }

    @Override
    public IncrementOption<byte[]> option() {
        return this;
    }

    public void setCommand(Command command) {
        this.req = command.getIncrReq();
    }

    @Override
    public OpType type() {
        return OpType.INCREMENT;
    }

    @Override
    public byte[] key() {
        if (null != key) {
            return key;
        }
        key = req.getKey().toByteArray();
        return key;
    }

    void reset() {
        req = null;
        key = null;
    }

    @Override
    public void close() {
        reset();
        recyclerHandle.recycle(this);
    }

    @Override
    public boolean getTotal() {
        return req.getGetTotal();
    }
}
