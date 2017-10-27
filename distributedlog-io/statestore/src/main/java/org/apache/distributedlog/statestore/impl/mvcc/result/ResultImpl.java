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

package org.apache.distributedlog.statestore.impl.mvcc.result;

import io.netty.util.Recycler.Handle;
import lombok.Setter;
import org.apache.distributedlog.statestore.api.mvcc.op.OpType;
import org.apache.distributedlog.statestore.api.mvcc.result.Code;
import org.apache.distributedlog.statestore.api.mvcc.result.Result;

/**
 * A base implementation of {@link Result}.
 */
@Setter
abstract class ResultImpl<K, V, T extends ResultImpl<K, V, T>> implements Result<K, V> {

    protected final Handle<T> handle;
    protected final OpType type;

    protected Code code;

    protected ResultImpl(OpType type, Handle<T> handle) {
        this.type = type;
        this.handle = handle;
    }

    abstract T asResult();

    @Override
    public OpType type() {
        return type;
    }

    @Override
    public Code code() {
        return code;
    }

    protected void reset() {
        this.code = null;
    }

    @Override
    public void recycle() {
        reset();

        this.handle.recycle(asResult());
    }
}
