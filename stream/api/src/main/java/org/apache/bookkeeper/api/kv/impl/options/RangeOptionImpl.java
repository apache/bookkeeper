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
import org.apache.bookkeeper.api.kv.options.RangeOption;

@Accessors(fluent = true)
@Getter
@ToString(exclude = "handle")
class RangeOptionImpl<K> implements RangeOption<K> {

    static <K> RangeOptionImpl<K> create(Recycler<RangeOptionImpl<K>> recycler) {
        return recycler.get();
    }

    private final Handle<RangeOptionImpl<K>> handle;

    private long limit;
    private long minModRev = Long.MIN_VALUE;
    private long maxModRev = Long.MAX_VALUE;
    private long minCreateRev = Long.MIN_VALUE;
    private long maxCreateRev = Long.MAX_VALUE;
    private boolean keysOnly;
    private boolean countOnly;
    private K endKey;

    RangeOptionImpl(Handle<RangeOptionImpl<K>> handle) {
        this.handle = handle;
    }

    void set(RangeOptionBuilderImpl<K> builderImpl) {
        this.limit = builderImpl.limit();
        this.minCreateRev = builderImpl.minCreateRev();
        this.maxCreateRev = builderImpl.maxCreateRev();
        this.minModRev = builderImpl.minModRev();
        this.maxModRev = builderImpl.maxModRev();
        this.keysOnly = builderImpl.keysOnly();
        this.countOnly = builderImpl.countOnly();
        release(this.endKey);
        this.endKey = retain(builderImpl.endKey());
    }

    @Override
    public void close() {
        this.limit = -1L;
        this.minModRev = Long.MIN_VALUE;
        this.maxModRev = Long.MAX_VALUE;
        this.minCreateRev = Long.MIN_VALUE;
        this.maxCreateRev = Long.MAX_VALUE;
        this.keysOnly = false;
        this.countOnly = false;
        release(endKey);
        this.endKey = null;

        handle.recycle(this);
    }
}
