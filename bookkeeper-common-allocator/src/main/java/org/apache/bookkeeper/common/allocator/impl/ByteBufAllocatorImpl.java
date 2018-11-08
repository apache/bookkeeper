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

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import java.util.function.Consumer;

import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ByteBufAllocator}.
 */
public class ByteBufAllocatorImpl extends AbstractByteBufAllocator implements ByteBufAllocator {

    private static final Logger log = LoggerFactory.getLogger(ByteBufAllocatorImpl.class);

    private final ByteBufAllocator pooledAllocator;
    private final ByteBufAllocator unpooledAllocator;
    private final PoolingPolicy poolingPolicy;
    private final OutOfMemoryPolicy outOfMemoryPolicy;
    private final Consumer<OutOfMemoryError> outOfMemoryListener;

    ByteBufAllocatorImpl(ByteBufAllocator pooledAllocator, ByteBufAllocator unpooledAllocator,
            PoolingPolicy poolingPolicy, int poolingConcurrency, OutOfMemoryPolicy outOfMemoryPolicy,
            Consumer<OutOfMemoryError> outOfMemoryListener,
            LeakDetectionPolicy leakDetectionPolicy) {
        super(poolingPolicy == PoolingPolicy.PooledDirect /* preferDirect */);

        this.poolingPolicy = poolingPolicy;
        this.outOfMemoryPolicy = outOfMemoryPolicy;
        if (outOfMemoryListener == null) {
            this.outOfMemoryListener = (v) -> {
                log.error("Unable to allocate memory", v);
            };
        } else {
            this.outOfMemoryListener = outOfMemoryListener;
        }

        if (poolingPolicy == PoolingPolicy.PooledDirect) {
            if (pooledAllocator == null) {
                this.pooledAllocator = new PooledByteBufAllocator(
                        true /* preferDirect */,
                        poolingConcurrency /* nHeapArena */,
                        poolingConcurrency /* nDirectArena */,
                        PooledByteBufAllocator.defaultPageSize(),
                        PooledByteBufAllocator.defaultMaxOrder(),
                        PooledByteBufAllocator.defaultTinyCacheSize(),
                        PooledByteBufAllocator.defaultSmallCacheSize(),
                        PooledByteBufAllocator.defaultNormalCacheSize(),
                        PooledByteBufAllocator.defaultUseCacheForAllThreads());
            } else {
                this.pooledAllocator = pooledAllocator;
            }
        } else {
            this.pooledAllocator = null;
        }

        this.unpooledAllocator = (unpooledAllocator != null) ? unpooledAllocator : UnpooledByteBufAllocator.DEFAULT;

        // The setting is static in Netty, so it will actually affect all
        // allocators
        switch (leakDetectionPolicy) {
        case Disabled:
            if (log.isDebugEnabled()) {
                log.debug("Disable Netty allocator leak detector");
            }
            ResourceLeakDetector.setLevel(Level.DISABLED);
            break;

        case Simple:
            log.info("Setting Netty allocator leak detector to Simple");
            ResourceLeakDetector.setLevel(Level.SIMPLE);
            break;

        case Advanced:
            log.info("Setting Netty allocator leak detector to Advanced");
            ResourceLeakDetector.setLevel(Level.ADVANCED);
            break;

        case Paranoid:
            log.info("Setting Netty allocator leak detector to Paranoid");
            ResourceLeakDetector.setLevel(Level.PARANOID);
            break;
        }
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        try {
            // There are few cases in which we ask explicitly for a pooled
            // heap buffer.
            ByteBufAllocator alloc = (poolingPolicy == PoolingPolicy.PooledDirect) ? pooledAllocator
                    : unpooledAllocator;
            return alloc.heapBuffer(initialCapacity, maxCapacity);
        } catch (OutOfMemoryError e) {
            outOfMemoryListener.accept(e);
            throw e;
        }
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        if (poolingPolicy == PoolingPolicy.PooledDirect) {
            try {
                return pooledAllocator.directBuffer(initialCapacity, maxCapacity);
            } catch (OutOfMemoryError e) {
                switch (outOfMemoryPolicy) {
                case ThrowException:
                    outOfMemoryListener.accept(e);
                    throw e;

                case FallbackToHeap:
                    try {
                        return unpooledAllocator.heapBuffer(initialCapacity, maxCapacity);
                    } catch (OutOfMemoryError e2) {
                        outOfMemoryListener.accept(e2);
                        throw e2;
                    }
                }
                return null;
            }
        } else {
            // Unpooled heap buffer. Force heap buffers because unpooled direct
            // buffers have very high overhead of allocation/reclaiming
            try {
                return unpooledAllocator.heapBuffer(initialCapacity, maxCapacity);
            } catch (OutOfMemoryError e) {
                outOfMemoryListener.accept(e);
                throw e;
            }
        }
    }

    @Override
    public boolean isDirectBufferPooled() {
        return pooledAllocator != null && pooledAllocator.isDirectBufferPooled();
    }
}
