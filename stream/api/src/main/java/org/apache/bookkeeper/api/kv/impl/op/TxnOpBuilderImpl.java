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

import com.google.common.collect.Lists;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.List;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.Op;
import org.apache.bookkeeper.api.kv.op.TxnOp;
import org.apache.bookkeeper.api.kv.op.TxnOpBuilder;

/**
 * The default implementation of {@link TxnOpBuilder}.
 * @param <K>
 * @param <V>
 */
class TxnOpBuilderImpl<K, V> implements TxnOpBuilder<K, V> {

    private final Handle<TxnOpBuilderImpl<K, V>> handle;

    private final Recycler<TxnOpImpl<K, V>> txnOpRecycler;
    private List<CompareOp<K, V>> compareOps;
    private List<Op<K, V>> successOps;
    private List<Op<K, V>> failureOps;

    TxnOpBuilderImpl(Handle<TxnOpBuilderImpl<K, V>> handle,
                     Recycler<TxnOpImpl<K, V>> txnOpRecycler) {
        this.handle = handle;
        this.txnOpRecycler = txnOpRecycler;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized TxnOpBuilder<K, V> If(CompareOp... cmps) {
        if (null == compareOps) {
            compareOps = Lists.newArrayList();
        }
        for (CompareOp<K, V> cmp : cmps) {
            compareOps.add(cmp);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized TxnOpBuilder<K, V> Then(Op... ops) {
        if (null == successOps) {
            successOps = Lists.newArrayList();
        }
        for (Op<K, V> op : ops) {
            successOps.add(op);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized TxnOpBuilder<K, V> Else(Op... ops) {
        if (null == failureOps) {
            failureOps = Lists.newArrayList();
        }
        for (Op<K, V> op : ops) {
            failureOps.add(op);
        }
        return this;
    }

    @Override
    public synchronized TxnOp<K, V> build() {
        try {
            return txnOpRecycler.get()
                .compareOps(compareOps)
                .successOps(successOps)
                .failureOps(failureOps);
        } finally {
            compareOps = null;
            successOps = null;
            failureOps = null;
            handle.recycle(this);
        }
    }

}
