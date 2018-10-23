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
package org.apache.bookkeeper.common.allocator;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.util.function.Consumer;

import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorBuilderImpl;

/**
 * Builder object to customize a ByteBuf allocator.
 */
public interface ByteBufAllocatorBuilder {
    /**
     * Creates a new {@link ByteBufAllocatorBuilder}.
     */
    static ByteBufAllocatorBuilder create() {
        return new ByteBufAllocatorBuilderImpl();
    }

    /**
     * Finalize the configured {@link ByteBufAllocator}.
     */
    ByteBufAllocator build();

    /**
     * Specify a custom allocator where the allocation requests should be
     * forwarded to.
     *
     * <p>Default is to use a new instance of {@link PooledByteBufAllocator}.
     */
    ByteBufAllocatorBuilder pooledAllocator(ByteBufAllocator pooledAllocator);

    /**
     * Specify a custom allocator where the allocation requests should be
     * forwarded to.
     *
     * <p>Default is to use {@link UnpooledByteBufAllocator#DEFAULT}.
     */
    ByteBufAllocatorBuilder unpooledAllocator(ByteBufAllocator unpooledAllocator);

    /**
     * Define the memory pooling policy.
     *
     * <p>Default is {@link PoolingPolicy#PooledDirect}
     */
    ByteBufAllocatorBuilder poolingPolicy(PoolingPolicy policy);

    /**
     * Controls the amount of concurrency for the memory pool.
     *
     * <p>Default is to have a number of allocator arenas equals to 2 * CPUS.
     *
     * <p>Decreasing this number will reduce the amount of memory overhead, at the
     * expense of increased allocation contention.
     */
    ByteBufAllocatorBuilder poolingConcurrency(int poolingConcurrency);

    /**
     * Define the OutOfMemory handling policy.
     *
     * <p>Default is {@link OutOfMemoryPolicy#FallbackToHeap}
     */
    ByteBufAllocatorBuilder outOfMemoryPolicy(OutOfMemoryPolicy policy);

    /**
     * Add a listener that is triggered whenever there is an allocation failure.
     *
     * <p>Application can use this to trigger alerting or process restarting.
     */
    ByteBufAllocatorBuilder outOfMemoryListener(Consumer<OutOfMemoryError> listener);

    /**
     * Enable the leak detection for the allocator.
     *
     * <p>Default is {@link LeakDetectionPolicy#Disabled}
     */
    ByteBufAllocatorBuilder leakDetectionPolicy(LeakDetectionPolicy leakDetectionPolicy);
}
