/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/**
 * Unit test for {@link WriteCache}.
 */
public class WriteCacheTest {

    private static final ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    @Test
    public void simple() throws Exception {
        WriteCache cache = new WriteCache(allocator, 10 * 1024);

        ByteBuf entry1 = allocator.buffer(1024);
        ByteBufUtil.writeUtf8(entry1, "entry-1");
        entry1.writerIndex(entry1.capacity());

        assertTrue(cache.isEmpty());
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());

        cache.put(1, 1, entry1);

        assertFalse(cache.isEmpty());
        assertEquals(1, cache.count());
        assertEquals(entry1.readableBytes(), cache.size());

        assertEquals(entry1, cache.get(1, 1));
        assertNull(cache.get(1, 2));
        assertNull(cache.get(2, 1));

        assertEquals(entry1, cache.getLastEntry(1));
        assertEquals(null, cache.getLastEntry(2));

        cache.clear();

        assertTrue(cache.isEmpty());
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());

        ReferenceCountUtil.release(entry1);
        cache.close();
    }

    @Test
    public void cacheFull() throws Exception {
        int cacheSize = 10 * 1024;
        int entrySize = 1024;
        int entriesCount = cacheSize / entrySize;

        WriteCache cache = new WriteCache(allocator, cacheSize);

        ByteBuf entry = allocator.buffer(entrySize);
        entry.writerIndex(entry.capacity());

        for (int i = 0; i < entriesCount; i++) {
            assertTrue(cache.put(1, i, entry));
        }

        assertFalse(cache.put(1, 11, entry));

        assertFalse(cache.isEmpty());
        assertEquals(entriesCount, cache.count());
        assertEquals(cacheSize, cache.size());

        AtomicInteger findCount = new AtomicInteger(0);
        cache.forEach((ledgerId, entryId, data) -> {
            findCount.incrementAndGet();
        });

        assertEquals(entriesCount, findCount.get());

        cache.deleteLedger(1);

        findCount.set(0);
        cache.forEach((ledgerId, entryId, data) -> {
            findCount.incrementAndGet();
        });

        assertEquals(0, findCount.get());

        ReferenceCountUtil.release(entry);
        cache.close();
    }

    @Test
    public void testMultipleSegments() {
        // Create cache with max size 1Mb and each segment is 16Kb
        WriteCache cache = new WriteCache(allocator, 1024 * 1024, 16 * 1024);

        ByteBuf entry = Unpooled.buffer(1024);
        entry.writerIndex(entry.capacity());

        for (int i = 0; i < 48; i++) {
            cache.put(1, i, entry);
        }

        assertEquals(48, cache.count());
        assertEquals(48 * 1024, cache.size());

        cache.close();
    }

    @Test
    public void testEmptyCache() throws IOException {
        WriteCache cache = new WriteCache(allocator, 1024 * 1024, 16 * 1024);

        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
        assertTrue(cache.isEmpty());

        AtomicLong foundEntries = new AtomicLong();
        cache.forEach((ledgerId, entryId, entry) -> {
            foundEntries.incrementAndGet();
        });

        assertEquals(0, foundEntries.get());
        cache.close();
    }

    @Test
    public void testMultipleWriters() throws Exception {
        // Create cache with max size 1Mb and each segment is 16Kb
        WriteCache cache = new WriteCache(allocator, 10 * 1024 * 1024, 16 * 1024);

        ExecutorService executor = Executors.newCachedThreadPool();

        int numThreads = 10;
        int entriesPerThread = 10 * 1024 / numThreads;

        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            int ledgerId = i;

            executor.submit(() -> {
                try {
                    barrier.await();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                } catch (BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }

                ByteBuf entry = Unpooled.buffer(1024);
                entry.writerIndex(entry.capacity());

                for (int entryId = 0; entryId < entriesPerThread; entryId++) {
                    assertTrue(cache.put(ledgerId, entryId, entry));
                }

                latch.countDown();
            });
        }

        // Wait for all tasks to be completed
        latch.await();

        // assertEquals(numThreads * entriesPerThread, cache.count());
        assertEquals(cache.count() * 1024, cache.size());

        // Verify entries by iterating over write cache
        AtomicLong currentLedgerId = new AtomicLong(0);
        AtomicLong currentEntryId = new AtomicLong(0);

        cache.forEach((ledgerId, entryId, entry) -> {
            assertEquals(currentLedgerId.get(), ledgerId);
            assertEquals(currentEntryId.get(), entryId);

            if (currentEntryId.incrementAndGet() == entriesPerThread) {
                currentLedgerId.incrementAndGet();
                currentEntryId.set(0);
            }
        });

        cache.close();
        executor.shutdown();
    }

    @Test
    public void testLedgerDeletion() throws IOException {
        WriteCache cache = new WriteCache(allocator, 1024 * 1024, 16 * 1024);

        ByteBuf entry = Unpooled.buffer(1024);
        entry.writerIndex(entry.capacity());

        for (long ledgerId = 0; ledgerId < 10; ledgerId++) {
            for (int entryId = 0; entryId < 10; entryId++) {
                cache.put(ledgerId, entryId, entry);
            }
        }

        assertEquals(100, cache.count());
        assertEquals(100 * 1024, cache.size());

        cache.deleteLedger(5);

        // Entries are not immediately deleted, just ignored on scan
        assertEquals(100, cache.count());
        assertEquals(100 * 1024, cache.size());

        // Verify entries by iterating over write cache
        AtomicLong currentLedgerId = new AtomicLong(0);
        AtomicLong currentEntryId = new AtomicLong(0);

        cache.forEach((ledgerId, entryId, e) -> {
            assertEquals(currentLedgerId.get(), ledgerId);
            assertEquals(currentEntryId.get(), entryId);

            if (currentEntryId.incrementAndGet() == 10) {
                currentLedgerId.incrementAndGet();
                currentEntryId.set(0);

                if (currentLedgerId.get() == 5) {
                    // Ledger 5 was deleted
                    currentLedgerId.incrementAndGet();
                }
            }
        });

        cache.close();
    }

    @Test
    public void testWriteReadsInMultipleSegments() {
        // Create cache with max size 4 KB and each segment is 128 bytes
        WriteCache cache = new WriteCache(allocator, 4 * 1024, 128);

        for (int i = 0; i < 48; i++) {
            boolean inserted = cache.put(1, i, Unpooled.wrappedBuffer(("test-" + i).getBytes()));
            assertTrue(inserted);
        }

        assertEquals(48, cache.count());

        for (int i = 0; i < 48; i++) {
            ByteBuf b = cache.get(1, i);

            assertEquals("test-" + i, b.toString(Charset.forName("UTF-8")));
        }

        cache.close();
    }

    @Test
    public void testHasEntry() {
        // Create cache with max size 4 KB and each segment is 128 bytes
        WriteCache cache = new WriteCache(allocator, 4 * 1024, 128);

        long ledgerId = 0xdede;
        for (int i = 0; i < 48; i++) {
            boolean inserted = cache.put(ledgerId, i, Unpooled.wrappedBuffer(("test-" + i).getBytes()));
            assertTrue(inserted);
        }

        assertEquals(48, cache.count());

        assertFalse(cache.hasEntry(0xfede, 1));
        assertFalse(cache.hasEntry(ledgerId, -1));
        for (int i = 0; i < 48; i++) {
            assertTrue(cache.hasEntry(ledgerId, i));
        }
        assertFalse(cache.hasEntry(ledgerId, 48));
    }

    @Test(expected = IOException.class)
    public void testForEachIOException() throws Exception {
        try (WriteCache cache = new WriteCache(allocator, 1024 * 1024, 16 * 1024)) {

            for (int i = 0; i < 48; i++) {
                boolean inserted = cache.put(1, i, Unpooled.wrappedBuffer(("test-" + i).getBytes()));
                assertTrue(inserted);
            }

            assertEquals(48, cache.count());

            cache.forEach(((ledgerId, entryId, entry) -> {
                throw new IOException("test throw IOException.");
            }));
        }
    }
}
