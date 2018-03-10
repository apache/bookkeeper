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

package org.apache.bookkeeper.api.kv.impl.result;

import static io.netty.util.ReferenceCountUtil.release;
import static io.netty.util.ReferenceCountUtil.retain;

import io.netty.util.Recycler.Handle;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.api.kv.op.OpType;
import org.apache.bookkeeper.api.kv.result.Code;
import org.apache.bookkeeper.api.kv.result.Result;

/**
 * A base implementation of {@link Result}.
 */
@Accessors(fluent = true, chain = true)
@Setter
@Getter
@ToString(exclude = "handle")
abstract class ResultImpl<K, V, T extends ResultImpl<K, V, T>> implements Result<K, V> {

    protected final Handle<T> handle;
    protected final OpType type;

    protected long revision;
    protected Code code;
    protected K pKey;

    protected ResultImpl(OpType type, Handle<T> handle) {
        this.type = type;
        this.handle = handle;
    }

    abstract T asResult();

    public ResultImpl<K, V, T> pKey(K pKey) {
        release(this.pKey);
        this.pKey = retain(pKey);
        return this;
    }

    protected void reset() {
        this.code = null;
        this.revision = -1L;
        this.code = Code.UNEXPECTED;
        release(pKey);
        this.pKey = null;
    }

    @Override
    public void close() {
        reset();

        this.handle.recycle(asResult());
    }
}
