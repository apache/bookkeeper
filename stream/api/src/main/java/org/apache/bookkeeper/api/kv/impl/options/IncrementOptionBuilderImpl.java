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
import org.apache.bookkeeper.api.kv.options.IncrementOption;
import org.apache.bookkeeper.api.kv.options.IncrementOptionBuilder;

/**
 * The default implementation of range option builder.
 */
@Accessors(fluent = true)
@Setter
@Getter(AccessLevel.PACKAGE)
class IncrementOptionBuilderImpl<K> implements IncrementOptionBuilder<K> {

    static <K> IncrementOptionBuilderImpl<K> create(Recycler<IncrementOptionBuilderImpl<K>> buildRecyler) {
        return buildRecyler.get();
    }

    private final Handle<IncrementOptionBuilderImpl<K>> handle;
    private final Recycler<IncrementOptionImpl<K>> optionRecycler;

    private boolean getTotal;

    IncrementOptionBuilderImpl(Handle<IncrementOptionBuilderImpl<K>> handle,
                               Recycler<IncrementOptionImpl<K>> optionRecycler) {
        this.handle = handle;
        this.optionRecycler = optionRecycler;
    }

    @Override
    public IncrementOption<K> build() {
        try {
            IncrementOptionImpl<K> option = IncrementOptionImpl.create(this.optionRecycler);
            option.set(this);
            return option;
        } finally {
            recycle();
        }
    }

    private void recycle() {
        this.getTotal = false;

        handle.recycle(this);
    }
}
