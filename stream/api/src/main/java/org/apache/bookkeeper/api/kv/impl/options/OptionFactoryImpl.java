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
import org.apache.bookkeeper.api.kv.options.DeleteOptionBuilder;
import org.apache.bookkeeper.api.kv.options.IncrementOptionBuilder;
import org.apache.bookkeeper.api.kv.options.OptionFactory;
import org.apache.bookkeeper.api.kv.options.PutOptionBuilder;
import org.apache.bookkeeper.api.kv.options.RangeOptionBuilder;

/**
 * The default implementation of option factory.
 */
public class OptionFactoryImpl<K> implements OptionFactory<K> {

    private final Recycler<RangeOptionImpl<K>> rangeOptionRecycler = new Recycler<RangeOptionImpl<K>>() {
        @Override
        protected RangeOptionImpl<K> newObject(Handle<RangeOptionImpl<K>> handle) {
            return new RangeOptionImpl<>(handle);
        }
    };

    private final Recycler<RangeOptionBuilderImpl<K>> rangeOptionBuilderRecycler =
        new Recycler<RangeOptionBuilderImpl<K>>() {
            @Override
            protected RangeOptionBuilderImpl<K> newObject(Handle<RangeOptionBuilderImpl<K>> handle) {
                return new RangeOptionBuilderImpl<>(handle, rangeOptionRecycler);
            }
        };

    private final Recycler<PutOptionImpl<K>> putOptionRecycler = new Recycler<PutOptionImpl<K>>() {
        @Override
        protected PutOptionImpl<K> newObject(Handle<PutOptionImpl<K>> handle) {
            return new PutOptionImpl<>(handle);
        }
    };

    private final Recycler<PutOptionBuilderImpl<K>> putOptionBuilderRecycler =
        new Recycler<PutOptionBuilderImpl<K>>() {
            @Override
            protected PutOptionBuilderImpl<K> newObject(Handle<PutOptionBuilderImpl<K>> handle) {
                return new PutOptionBuilderImpl<>(handle, putOptionRecycler);
            }
        };

    private final Recycler<DeleteOptionImpl<K>> deleteOptionRecycler = new Recycler<DeleteOptionImpl<K>>() {
        @Override
        protected DeleteOptionImpl<K> newObject(Handle<DeleteOptionImpl<K>> handle) {
            return new DeleteOptionImpl<>(handle);
        }
    };

    private final Recycler<DeleteOptionBuilderImpl<K>> deleteOptionBuilderRecycler =
        new Recycler<DeleteOptionBuilderImpl<K>>() {
            @Override
            protected DeleteOptionBuilderImpl<K> newObject(Handle<DeleteOptionBuilderImpl<K>> handle) {
                return new DeleteOptionBuilderImpl<>(handle, deleteOptionRecycler);
            }
        };

    private final Recycler<IncrementOptionImpl<K>> incrementOptionRecycler = new Recycler<IncrementOptionImpl<K>>() {
        @Override
        protected IncrementOptionImpl<K> newObject(Handle<IncrementOptionImpl<K>> handle) {
            return new IncrementOptionImpl<>(handle);
        }
    };

    private final Recycler<IncrementOptionBuilderImpl<K>> incrementOptionBuilderRecycler =
        new Recycler<IncrementOptionBuilderImpl<K>>() {
            @Override
            protected IncrementOptionBuilderImpl<K> newObject(Handle<IncrementOptionBuilderImpl<K>> handle) {
                return new IncrementOptionBuilderImpl<>(handle, incrementOptionRecycler);
            }
        };


    @Override
    public PutOptionBuilder<K> newPutOption() {
        return PutOptionBuilderImpl.create(putOptionBuilderRecycler);
    }

    @Override
    public RangeOptionBuilder<K> newRangeOption() {
        return RangeOptionBuilderImpl.create(rangeOptionBuilderRecycler);
    }

    @Override
    public DeleteOptionBuilder<K> newDeleteOption() {
        return DeleteOptionBuilderImpl.create(deleteOptionBuilderRecycler);
    }

    @Override
    public IncrementOptionBuilder<K> newIncrementOption() {
        return IncrementOptionBuilderImpl.create(incrementOptionBuilderRecycler);
    }
}
