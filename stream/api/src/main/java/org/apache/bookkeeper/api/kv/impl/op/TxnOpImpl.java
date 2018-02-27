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
package org.apache.bookkeeper.api.kv.impl.op;

import io.netty.util.Recycler.Handle;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.Op;
import org.apache.bookkeeper.api.kv.op.OpType;
import org.apache.bookkeeper.api.kv.op.TxnOp;

@Accessors(fluent = true, chain = true)
@Getter
@Setter(AccessLevel.PACKAGE)
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@ToString(exclude = "handle")
class TxnOpImpl<K, V> implements TxnOp<K, V> {

    private final Handle<TxnOpImpl<K, V>> handle;

    private List<CompareOp<K, V>> compareOps;
    private List<Op<K, V>> successOps;
    private List<Op<K, V>> failureOps;

    @Override
    public List<CompareOp<K, V>> compareOps() {
        return compareOps;
    }

    @Override
    public List<Op<K, V>> successOps() {
        return successOps;
    }

    @Override
    public List<Op<K, V>> failureOps() {
        return failureOps;
    }

    @Override
    public OpType type() {
        return OpType.TXN;
    }

    @Override
    public void close() {
        if (null != compareOps) {
            compareOps.forEach(CompareOp::close);
            compareOps = null;
        }
        if (null != successOps) {
            successOps.forEach(Op::close);
            successOps = null;
        }
        if (null != failureOps) {
            failureOps.forEach(Op::close);
            failureOps = null;
        }

        handle.recycle(this);
    }
}
