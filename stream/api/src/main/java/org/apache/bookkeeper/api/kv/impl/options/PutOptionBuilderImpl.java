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

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.api.kv.options.PutOption;
import org.apache.bookkeeper.api.kv.options.PutOptionBuilder;

/**
 * The default implementation of range option builder.
 */
@Accessors(fluent = true)
@Setter
@Getter(AccessLevel.PACKAGE)
class PutOptionBuilderImpl<K> implements PutOptionBuilder<K> {

    static <K> PutOptionBuilderImpl<K> create(Recycler<PutOptionBuilderImpl<K>> buildRecyler) {
        return buildRecyler.get();
    }

    private final Handle<PutOptionBuilderImpl<K>> handle;
    private final Recycler<PutOptionImpl<K>> optionRecycler;

    private boolean prevKv;

    PutOptionBuilderImpl(Handle<PutOptionBuilderImpl<K>> handle,
                         Recycler<PutOptionImpl<K>> optionRecycler) {
        this.handle = handle;
        this.optionRecycler = optionRecycler;
    }

    @Override
    public PutOption<K> build() {
        try {
            PutOptionImpl<K> option = PutOptionImpl.create(this.optionRecycler);
            option.set(this);
            return option;
        } finally {
            recycle();
        }
    }

    private void recycle() {
        this.prevKv = false;

        handle.recycle(this);
    }
}
