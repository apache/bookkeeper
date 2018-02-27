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
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.api.kv.op.CompareOp;
import org.apache.bookkeeper.api.kv.op.CompareResult;
import org.apache.bookkeeper.api.kv.op.CompareTarget;

@Accessors(fluent = true, chain = true)
@Getter
@Setter(AccessLevel.PACKAGE)
@ToString(exclude = "handle")
class CompareOpImpl<K, V> implements CompareOp<K, V> {

    private final Handle<CompareOpImpl<K, V>> handle;

    private CompareTarget target;
    private CompareResult result;
    private K key;
    private V value;
    private long revision;

    CompareOpImpl(Handle<CompareOpImpl<K, V>> handle) {
        this.handle = handle;
    }

    CompareOpImpl<K, V> key(K key) {
        release(this.key);
        this.key = retain(key);
        return this;
    }

    CompareOpImpl<K, V> value(V value) {
        release(this.value);
        this.value = retain(value);
        return this;
    }

    @Override
    public void close() {
        this.target = null;
        this.result = null;
        release(key);
        this.key = null;
        release(value);
        this.value = null;
        this.revision = -1L;
    }
}
