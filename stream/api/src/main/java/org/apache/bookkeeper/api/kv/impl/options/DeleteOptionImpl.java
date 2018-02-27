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
package org.apache.bookkeeper.api.kv.impl.options;

import static io.netty.util.ReferenceCountUtil.release;
import static io.netty.util.ReferenceCountUtil.retain;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.api.kv.options.DeleteOption;

@Accessors(fluent = true)
@Getter
@ToString(exclude = "handle")
class DeleteOptionImpl<K> implements DeleteOption<K> {

    static <K> DeleteOptionImpl<K> create(Recycler<DeleteOptionImpl<K>> recycler) {
        return recycler.get();
    }

    private final Handle<DeleteOptionImpl<K>> handle;

    private boolean prevKv;
    private K endKey;

    DeleteOptionImpl(Handle<DeleteOptionImpl<K>> handle) {
        this.handle = handle;
    }

    void set(DeleteOptionBuilderImpl<K> builderImpl) {
        this.prevKv = builderImpl.prevKv();
        release(this.endKey);
        this.endKey = retain(builderImpl.endKey());
    }

    @Override
    public void close() {
        this.prevKv = false;
        release(endKey);
        this.endKey = null;

        handle.recycle(this);
    }
}
