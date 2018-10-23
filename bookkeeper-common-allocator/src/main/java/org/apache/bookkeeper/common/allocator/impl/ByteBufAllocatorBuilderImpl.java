/**
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
package org.apache.bookkeeper.common.allocator.impl;

import io.netty.buffer.ByteBufAllocator;

import java.util.function.Consumer;

import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;

/**
 * Implementation of {@link ByteBufAllocatorBuilder}.
 */
public class ByteBufAllocatorBuilderImpl implements ByteBufAllocatorBuilder {

    ByteBufAllocator pooledAllocator = null;
    ByteBufAllocator unpooledAllocator = null;
    PoolingPolicy poolingPolicy = PoolingPolicy.PooledDirect;
    int poolingConcurrency = 2 * Runtime.getRuntime().availableProcessors();
    OutOfMemoryPolicy outOfMemoryPolicy = OutOfMemoryPolicy.FallbackToHeap;
    Consumer<OutOfMemoryError> outOfMemoryListener = null;
    LeakDetectionPolicy leakDetectionPolicy = LeakDetectionPolicy.Disabled;

    @Override
    public ByteBufAllocator build() {
        return new ByteBufAllocatorImpl(pooledAllocator, unpooledAllocator, poolingPolicy, poolingConcurrency,
                outOfMemoryPolicy, outOfMemoryListener, leakDetectionPolicy);
    }

    @Override
    public ByteBufAllocatorBuilder pooledAllocator(ByteBufAllocator pooledAllocator) {
        this.pooledAllocator = pooledAllocator;
        return this;
    }

    @Override
    public ByteBufAllocatorBuilder unpooledAllocator(ByteBufAllocator unpooledAllocator) {
        this.unpooledAllocator = unpooledAllocator;
        return this;
    }

    @Override
    public ByteBufAllocatorBuilder poolingPolicy(PoolingPolicy policy) {
        this.poolingPolicy = policy;
        return this;
    }

    @Override
    public ByteBufAllocatorBuilder poolingConcurrency(int poolingConcurrency) {
        this.poolingConcurrency = poolingConcurrency;
        return this;
    }

    @Override
    public ByteBufAllocatorBuilder outOfMemoryPolicy(OutOfMemoryPolicy policy) {
        this.outOfMemoryPolicy = policy;
        return this;
    }

    @Override
    public ByteBufAllocatorBuilder outOfMemoryListener(Consumer<OutOfMemoryError> listener) {
        this.outOfMemoryListener = listener;
        return this;
    }

    @Override
    public ByteBufAllocatorBuilder leakDetectionPolicy(LeakDetectionPolicy leakDetectionPolicy) {
        this.leakDetectionPolicy = leakDetectionPolicy;
        return this;
    }

}
