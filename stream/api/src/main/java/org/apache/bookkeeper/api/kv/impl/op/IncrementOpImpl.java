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

import static io.netty.util.ReferenceCountUtil.release;
import static io.netty.util.ReferenceCountUtil.retain;

import io.netty.util.Recycler.Handle;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.api.kv.op.IncrementOp;
import org.apache.bookkeeper.api.kv.op.OpType;
import org.apache.bookkeeper.api.kv.options.IncrementOption;

@Accessors(fluent = true, chain = true)
@Getter
@Setter(AccessLevel.PACKAGE)
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@ToString(exclude = "handle")
class IncrementOpImpl<K, V> implements IncrementOp<K, V> {

    private final Handle<IncrementOpImpl<K, V>> handle;

    private K key;
    private long amount;
    private IncrementOption<K> option;

    @Override
    public OpType type() {
        return OpType.INCREMENT;
    }

    IncrementOpImpl<K, V> key(K key) {
        release(this.key);
        this.key = retain(key);
        return this;
    }

    IncrementOpImpl<K, V> amount(long amount) {
        this.amount = amount;
        return this;
    }

    @Override
    public void close() {
        release(key);
        this.key = null;
        this.amount = 0L;
        if (null != option) {
            option.close();
            option = null;
        }

        handle.recycle(this);
    }
}
