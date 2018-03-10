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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.DeleteOptionBuilder;

/**
 * The default implementation of delete option builder.
 */
@Accessors(fluent = true)
@Setter
@Getter(AccessLevel.PACKAGE)
class DeleteOptionBuilderImpl<K> implements DeleteOptionBuilder<K> {

    static <K> DeleteOptionBuilderImpl<K> create(Recycler<DeleteOptionBuilderImpl<K>> buildRecyler) {
        return buildRecyler.get();
    }

    private final Handle<DeleteOptionBuilderImpl<K>> handle;
    private final Recycler<DeleteOptionImpl<K>> optionRecycler;

    private boolean prevKv;
    private K endKey;

    DeleteOptionBuilderImpl(Handle<DeleteOptionBuilderImpl<K>> handle,
                            Recycler<DeleteOptionImpl<K>> optionRecycler) {
        this.handle = handle;
        this.optionRecycler = optionRecycler;
    }

    @Override
    public DeleteOptionBuilderImpl<K> endKey(K endKey) {
        // release previous key
        release(this.endKey);
        this.endKey = retain(endKey);
        return this;
    }

    @Override
    public DeleteOption<K> build() {
        try {
            DeleteOptionImpl<K> option = DeleteOptionImpl.create(this.optionRecycler);
            option.set(this);
            return option;
        } finally {
            recycle();
        }
    }

    private void recycle() {
        this.prevKv = false;
        release(endKey);
        this.endKey = null;

        handle.recycle(this);
    }
}
