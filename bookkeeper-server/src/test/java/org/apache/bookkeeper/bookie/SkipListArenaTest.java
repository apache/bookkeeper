/**
 *
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
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.primitives.Ints;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.bookie.SkipListArena.MemorySlice;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Test;

/**
 * Test the SkipListArena class.
 */
public class SkipListArenaTest {

    class CustomConfiguration extends ServerConfiguration {
        @Override
        public int getSkipListArenaChunkSize() {
            return 4096;
        }
        @Override
        public int getSkipListArenaMaxAllocSize() {
            return 1024;
        }
        @Override
        public boolean getJournalFlushWhenQueueEmpty() {
            return true;
        }

    }

    final CustomConfiguration cfg = new CustomConfiguration();

    /**
    * Test random allocations.
    */
    @Test
    public void testRandomAllocation() {
        Random rand = new Random();
        SkipListArena arena = new SkipListArena(cfg);
        int expectedOff = 0;
        byte[] lastBuffer = null;

        // 10K iterations by 0-512 alloc -> 2560kB expected
        // should be reasonable for unit test and also cover wraparound
        // behavior
        for (int i = 0; i < 10000; i++) {
            int size = rand.nextInt(512);
            MemorySlice alloc = arena.allocateBytes(size);

            if (alloc.getData() != lastBuffer) {
                expectedOff = 0;
                lastBuffer = alloc.getData();
            }
            assertTrue(expectedOff == alloc.getOffset());
            assertTrue("Allocation " + alloc + " overruns buffer",
              alloc.getOffset() + size <= alloc.getData().length);
            expectedOff += size;
        }
    }

    @Test
    public void testLargeAllocation() {
        SkipListArena arena = new SkipListArena(cfg);
        MemorySlice alloc = arena.allocateBytes(1024 + 1024);
        assertNull("2KB allocation shouldn't be satisfied by LAB.", alloc);
    }

    private class ByteArray {
        final byte[] bytes;
        ByteArray(final byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int hashCode() {
            return bytes.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (object instanceof ByteArray) {
                ByteArray other = (ByteArray) object;
                return this.bytes.equals(other.bytes);
            }
            return false;
        }
    }

    private static class AllocBuffer implements Comparable<AllocBuffer>{
        private final MemorySlice alloc;
        private final int size;
        public AllocBuffer(MemorySlice alloc, int size) {
            super();
            this.alloc = alloc;
            this.size = size;
        }

        @Override
        public int compareTo(AllocBuffer e) {
            assertTrue(alloc.getData() == e.alloc.getData());
            return Ints.compare(alloc.getOffset(), e.alloc.getOffset());
        }

        @Override
        public String toString() {
          return alloc + ":" + size;
        }
    }

    private Thread getAllocThread(final ConcurrentLinkedQueue<AllocBuffer> queue,
                                  final CountDownLatch latch,
                                  final SkipListArena arena) {
        return new Thread(new Runnable() {
            @Override
            public void run() {
                Random rand = new Random();
                for (int j = 0; j < 1000; j++) {
                    int size = rand.nextInt(512);
                    MemorySlice alloc = arena.allocateBytes(size);
                    queue.add(new AllocBuffer(alloc, size));
                }
                latch.countDown();
            }
        });
    }

    /**
    * Test concurrent allocation, check the results don't overlap.
    */
    @Test
    public void testConcurrency() throws Exception {
        final SkipListArena arena = new SkipListArena(cfg);
        final CountDownLatch latch = new CountDownLatch(10);
        final ConcurrentLinkedQueue<AllocBuffer> queue = new ConcurrentLinkedQueue<AllocBuffer>();

        Set<Thread> testThreads = new HashSet<Thread>();
        for (int i = 0; i < 10; i++) {
            testThreads.add(getAllocThread(queue, latch, arena));
        }

        for (Thread thread : testThreads) {
            thread.start();
        }
        latch.await();

        // Partition the allocations by the actual byte[] they share,
        // make sure offsets are unique and non-overlap for each buffer.
        Map<ByteArray, Map<Integer, AllocBuffer>> mapsByArray = new HashMap<ByteArray, Map<Integer, AllocBuffer>>();
        boolean overlapped = false;

        final AllocBuffer[] buffers = queue.toArray(new AllocBuffer[0]);
        for (AllocBuffer buf : buffers) {
            if (buf.size != 0) {
                ByteArray ptr = new ByteArray(buf.alloc.getData());
                Map<Integer, AllocBuffer> treeMap = mapsByArray.get(ptr);
                if (treeMap == null) {
                    treeMap = new TreeMap<Integer, AllocBuffer>();
                    mapsByArray.put(ptr, treeMap);
                }
                AllocBuffer other = treeMap.put(buf.alloc.getOffset(), buf);
                if (other != null) {
                    fail("Buffer " + other + " overlapped with " + buf);
                }
            }
        }

        // Now check each byte array to make sure allocations don't overlap
        for (Map<Integer, AllocBuffer> treeMap : mapsByArray.values()) {
            int expectedOff = 0;
            for (AllocBuffer buf : treeMap.values()) {
                assertEquals(expectedOff, buf.alloc.getOffset());
                expectedOff += buf.size;
            }
        }
    }
}

