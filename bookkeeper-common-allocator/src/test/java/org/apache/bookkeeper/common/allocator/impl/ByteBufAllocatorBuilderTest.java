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
package org.apache.bookkeeper.common.allocator.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.junit.Test;

/**
 * Tests for {@link ByteBufAllocatorBuilderImpl}.
 */
public class ByteBufAllocatorBuilderTest {

    private static final OutOfMemoryError outOfDirectMemException;

    static {
        try {
            Class<?> clazz = (Class<?>) ByteBufAllocatorBuilderTest.class.getClassLoader()
                    .loadClass("io.netty.util.internal.OutOfDirectMemoryError");
            @SuppressWarnings("unchecked")
            Constructor<OutOfMemoryError> constructor = (Constructor<OutOfMemoryError>) clazz
                    .getDeclaredConstructor(String.class);

            constructor.setAccessible(true);
            outOfDirectMemException = constructor.newInstance("no mem");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testOomWithException() {
        ByteBufAllocator baseAlloc = mock(ByteBufAllocator.class);
        when(baseAlloc.directBuffer(anyInt(), anyInt())).thenThrow(outOfDirectMemException);

        AtomicReference<OutOfMemoryError> receivedException = new AtomicReference<>();

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .pooledAllocator(baseAlloc)
                .outOfMemoryPolicy(OutOfMemoryPolicy.ThrowException)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();

        try {
            alloc.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            // Expected
            assertEquals(outOfDirectMemException, e);
        }

        // Ensure the notification was triggered even when exception is thrown
        assertEquals(outOfDirectMemException, receivedException.get());
    }

    @Test
    public void testOomWithFallback() {
        ByteBufAllocator baseAlloc = mock(ByteBufAllocator.class);
        when(baseAlloc.directBuffer(anyInt(), anyInt())).thenThrow(outOfDirectMemException);

        AtomicReference<OutOfMemoryError> receivedException = new AtomicReference<>();

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .pooledAllocator(baseAlloc)
                .unpooledAllocator(UnpooledByteBufAllocator.DEFAULT)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();

        // Should not throw exception
        ByteBuf buf = alloc.buffer();
        assertEquals(UnpooledByteBufAllocator.DEFAULT, buf.alloc());

        // No notification should have been triggered
        assertEquals(null, receivedException.get());
    }

    @Test
    public void testOomWithFallbackAndNoMoreHeap() {
        ByteBufAllocator baseAlloc = mock(ByteBufAllocator.class);
        when(baseAlloc.directBuffer(anyInt(), anyInt())).thenThrow(outOfDirectMemException);

        ByteBufAllocator heapAlloc = mock(ByteBufAllocator.class);
        OutOfMemoryError noHeapError = new OutOfMemoryError("no more heap");
        when(heapAlloc.heapBuffer(anyInt(), anyInt())).thenThrow(noHeapError);

        AtomicReference<OutOfMemoryError> receivedException = new AtomicReference<>();

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .pooledAllocator(baseAlloc)
                .unpooledAllocator(heapAlloc)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();

        try {
            alloc.buffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            // Expected
            assertEquals(noHeapError, e);
        }

        // Ensure the notification was triggered even when exception is thrown
        assertEquals(noHeapError, receivedException.get());
    }

    @Test
    public void testOomUnpooledDirect() {
        ByteBufAllocator heapAlloc = mock(ByteBufAllocator.class);
        OutOfMemoryError noMemError = new OutOfMemoryError("no more direct mem");
        when(heapAlloc.directBuffer(anyInt(), anyInt())).thenThrow(noMemError);

        AtomicReference<OutOfMemoryError> receivedException = new AtomicReference<>();

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)
                .unpooledAllocator(heapAlloc)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();

        try {
            alloc.directBuffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            // Expected
            assertEquals(noMemError, e);
        }

        // Ensure the notification was triggered even when exception is thrown
        assertEquals(noMemError, receivedException.get());
    }

    @Test
    public void testOomUnpooledWithHeap() {
        ByteBufAllocator heapAlloc = mock(ByteBufAllocator.class);
        OutOfMemoryError noHeapError = new OutOfMemoryError("no more heap");
        when(heapAlloc.heapBuffer(anyInt(), anyInt())).thenThrow(noHeapError);

        AtomicReference<OutOfMemoryError> receivedException = new AtomicReference<>();

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)
                .unpooledAllocator(heapAlloc)
                .outOfMemoryPolicy(OutOfMemoryPolicy.FallbackToHeap)
                .outOfMemoryListener((e) -> {
                    receivedException.set(e);
                })
                .build();

        try {
            alloc.heapBuffer();
            fail("Should have thrown exception");
        } catch (OutOfMemoryError e) {
            // Expected
            assertEquals(noHeapError, e);
        }

        // Ensure the notification was triggered even when exception is thrown
        assertEquals(noHeapError, receivedException.get());
    }

    @Test
    public void testUnpooled() {
        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.UnpooledHeap)
                .build();

        ByteBuf buf = alloc.buffer();
        assertEquals(UnpooledByteBufAllocator.DEFAULT, buf.alloc());
        assertTrue(buf.hasArray());

        ByteBuf buf2 = alloc.directBuffer();
        assertEquals(UnpooledByteBufAllocator.DEFAULT, buf2.alloc());
        assertFalse(buf2.hasArray());
    }

    @Test
    public void testPooled() {
        PooledByteBufAllocator pooledAlloc = new PooledByteBufAllocator(true);

        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .pooledAllocator(pooledAlloc)
                .build();

        assertTrue(alloc.isDirectBufferPooled());

        ByteBuf buf1 = alloc.buffer();
        assertEquals(pooledAlloc, buf1.alloc());
        assertFalse(buf1.hasArray());
        ReferenceCountUtil.release(buf1);

        ByteBuf buf2 = alloc.directBuffer();
        assertEquals(pooledAlloc, buf2.alloc());
        assertFalse(buf2.hasArray());
        ReferenceCountUtil.release(buf2);

        ByteBuf buf3 = alloc.heapBuffer();
        assertEquals(pooledAlloc, buf3.alloc());
        assertTrue(buf3.hasArray());
        ReferenceCountUtil.release(buf3);
    }

    @Test
    public void testPooledWithDefaultAllocator() {
        ByteBufAllocator alloc = ByteBufAllocatorBuilder.create()
                .poolingPolicy(PoolingPolicy.PooledDirect)
                .poolingConcurrency(3)
                .build();

        assertTrue(alloc.isDirectBufferPooled());

        ByteBuf buf1 = alloc.buffer();
        assertEquals(PooledByteBufAllocator.class, buf1.alloc().getClass());
        assertEquals(3, ((PooledByteBufAllocator) buf1.alloc()).metric().numDirectArenas());
        assertFalse(buf1.hasArray());
        ReferenceCountUtil.release(buf1);

        ByteBuf buf2 = alloc.directBuffer();
        assertFalse(buf2.hasArray());
        ReferenceCountUtil.release(buf2);

        ByteBuf buf3 = alloc.heapBuffer();
        assertTrue(buf3.hasArray());
        ReferenceCountUtil.release(buf3);
    }
}
