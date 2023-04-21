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
import org.apache.bookkeeper.api.kv.options.RangeOption;
import org.apache.bookkeeper.api.kv.options.RangeOptionBuilder;

/**
 * The default implementation of range option builder.
 */
@Accessors(fluent = true)
@Setter
@Getter(AccessLevel.PACKAGE)
class RangeOptionBuilderImpl<K> implements RangeOptionBuilder<K> {

    static <K> RangeOptionBuilderImpl<K> create(Recycler<RangeOptionBuilderImpl<K>> buildRecyler) {
        return buildRecyler.get();
    }

    private final Handle<RangeOptionBuilderImpl<K>> handle;
    private final Recycler<RangeOptionImpl<K>> optionRecycler;

    private long limit = -1L;
    private long minModRev = Long.MIN_VALUE;
    private long maxModRev = Long.MAX_VALUE;
    private long minCreateRev = Long.MIN_VALUE;
    private long maxCreateRev = Long.MAX_VALUE;
    private boolean keysOnly;
    private boolean countOnly;
    private K endKey;

    RangeOptionBuilderImpl(Handle<RangeOptionBuilderImpl<K>> handle,
                           Recycler<RangeOptionImpl<K>> optionRecycler) {
        this.handle = handle;
        this.optionRecycler = optionRecycler;
    }

    @Override
    public RangeOptionBuilderImpl<K> endKey(K endKey) {
        // release previous key
        release(this.endKey);
        this.endKey = retain(endKey);
        return this;
    }

    @Override
    public RangeOption<K> build() {
        try {
            RangeOptionImpl<K> option = RangeOptionImpl.create(this.optionRecycler);
            option.set(this);
            return option;
        } finally {
            recycle();
        }
    }

    private void recycle() {
        this.limit = -1L;
        this.minCreateRev = Long.MIN_VALUE;
        this.minModRev = Long.MIN_VALUE;
        this.maxCreateRev = Long.MAX_VALUE;
        this.maxModRev = Long.MAX_VALUE;
        this.keysOnly = false;
        this.countOnly = false;
        release(endKey);
        this.endKey = null;

        handle.recycle(this);
    }
}
