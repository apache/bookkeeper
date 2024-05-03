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
package org.apache.bookkeeper.util.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Test the ConcurrentLongHashSet class.
 */
@SuppressWarnings("deprecation")
public class ConcurrentLongHashSetTest {

    @Test
    public void testConstructor() {
        try {
            ConcurrentLongHashSet.newBuilder().concurrencyLevel(0).build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentLongHashSet.newBuilder().expectedItems(16).concurrencyLevel(0).build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentLongHashSet.newBuilder().expectedItems(4).concurrencyLevel(8).build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void simpleInsertions() {
        ConcurrentLongHashSet set = ConcurrentLongHashSet.newBuilder()
                .expectedItems(16)
                .build();

        assertTrue(set.isEmpty());
        assertTrue(set.add(1));
        assertFalse(set.isEmpty());

        assertTrue(set.add(2));
        assertTrue(set.add(3));

        assertEquals(set.size(), 3);

        assertTrue(set.contains(1));
        assertEquals(set.size(), 3);

        assertTrue(set.remove(1));
        assertEquals(set.size(), 2);
        assertFalse(set.contains(1));
        assertFalse(set.contains(5));
        assertEquals(set.size(), 2);

        assertTrue(set.add(1));
        assertEquals(set.size(), 3);
        assertFalse(set.add(1));
        assertEquals(set.size(), 3);
    }

    @Test
    public void testReduceUnnecessaryExpansions() {
        ConcurrentLongHashSet set = ConcurrentLongHashSet.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .build();
        assertTrue(set.add(1));
        assertTrue(set.add(2));
        assertTrue(set.add(3));
        assertTrue(set.add(4));

        assertTrue(set.remove(1));
        assertTrue(set.remove(2));
        assertTrue(set.remove(3));
        assertTrue(set.remove(4));

        assertEquals(0, set.getUsedBucketCount());
    }

    @Test
    public void testRemove() {
        ConcurrentLongHashSet set = ConcurrentLongHashSet.newBuilder().build();

        assertTrue(set.isEmpty());
        assertTrue(set.add(1));
        assertFalse(set.isEmpty());

        assertFalse(set.remove(0));
        assertFalse(set.isEmpty());
        assertTrue(set.remove(1));
        assertTrue(set.isEmpty());
    }

    @Test
    public void testRehashing() {
        int n = 16;
        ConcurrentLongHashSet set = ConcurrentLongHashSet.newBuilder()
                .expectedItems(n / 2)
                .concurrencyLevel(1)
                .build();
        assertEquals(set.capacity(), n);
        assertEquals(set.size(), 0);

        for (int i = 0; i < n; i++) {
            set.add(i);
        }

        assertEquals(set.capacity(), 2 * n);
        assertEquals(set.size(), n);
    }

    @Test
    public void testRehashingWithDeletes() {
        int n = 16;
        ConcurrentLongHashSet set = ConcurrentLongHashSet.newBuilder()
                .expectedItems(n / 2)
                .concurrencyLevel(1)
                .build();
        assertEquals(set.capacity(), n);
        assertEquals(set.size(), 0);

        for (int i = 0; i < n / 2; i++) {
            set.add(i);
        }

        for (int i = 0; i < n / 2; i++) {
            set.remove(i);
        }

        for (int i = n; i < (2 * n); i++) {
            set.add(i);
        }

        assertEquals(set.capacity(), 2 * n);
        assertEquals(set.size(), n);
    }

    @Test
    public void concurrentInsertions() throws Throwable {
        ConcurrentLongHashSet set = ConcurrentLongHashSet.newBuilder().build();
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int n = 100_000;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                for (int j = 0; j < n; j++) {
                    long key = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
                    // Ensure keys are unique
                    key -= key % (threadIdx + 1);

                    set.add(key);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(set.size(), n * nThreads);

        executor.shutdown();
    }

    @Test
    public void concurrentInsertionsAndReads() throws Throwable {
        ConcurrentLongHashSet map = ConcurrentLongHashSet.newBuilder().build();
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int n = 100_000;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                for (int j = 0; j < n; j++) {
                    long key = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
                    // Ensure keys are unique
                    key -= key % (threadIdx + 1);

                    map.add(key);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals(map.size(), n * nThreads);

        executor.shutdown();
    }

    @Test
    public void testClear() {
        ConcurrentLongHashSet map = ConcurrentLongHashSet.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertEquals(4, map.capacity());

        assertTrue(map.add(1));
        assertTrue(map.add(2));
        assertTrue(map.add(3));

        assertEquals(8, map.capacity());
        map.clear();
        assertEquals(4, map.capacity());
    }

    @Test
    public void testExpandAndShrink() {
        ConcurrentLongHashSet map = ConcurrentLongHashSet.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertEquals(4, map.capacity());

        assertTrue(map.add(1));
        assertTrue(map.add(2));
        assertTrue(map.add(3));

        // expand hashmap
        assertEquals(8, map.capacity());

        assertTrue(map.remove(1));
        // not shrink
        assertEquals(8, map.capacity());
        assertTrue(map.remove(2));
        // shrink hashmap
        assertEquals(4, map.capacity());

        // expand hashmap
        assertTrue(map.add(4));
        assertTrue(map.add(5));
        assertEquals(8, map.capacity());

        //verify that the map does not keep shrinking at every remove() operation
        assertTrue(map.add(6));
        assertTrue(map.remove(6));
        assertEquals(8, map.capacity());
    }

    @Test
    public void testConcurrentExpandAndShrinkAndGet()  throws Throwable {
        ConcurrentLongHashSet set = ConcurrentLongHashSet.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertEquals(set.capacity(), 4);

        ExecutorService executor = Executors.newCachedThreadPool();
        final int readThreads = 16;
        final int writeThreads = 1;
        final int n = 1_000;
        CyclicBarrier barrier = new CyclicBarrier(writeThreads + readThreads);
        Future<?> future = null;
        AtomicReference<Exception> ex = new AtomicReference<>();

        for (int i = 0; i < readThreads; i++) {
            executor.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                while (true) {
                    try {
                        set.contains(1);
                    } catch (Exception e) {
                        ex.set(e);
                    }
                }
            });
        }

        assertTrue(set.add(1));
        future = executor.submit(() -> {
            try {
                barrier.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            for (int i = 0; i < n; i++) {
                // expand hashmap
                assertTrue(set.add(2));
                assertTrue(set.add(3));
                assertEquals(set.capacity(), 8);

                // shrink hashmap
                assertTrue(set.remove(2));
                assertTrue(set.remove(3));
                assertEquals(set.capacity(), 4);
            }
        });

        future.get();
        assertNull(ex.get());
        // shut down pool
        executor.shutdown();
    }

    @Test
    public void testExpandShrinkAndClear() {
        ConcurrentLongHashSet map = ConcurrentLongHashSet.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        final long initCapacity = map.capacity();
        assertEquals(4, map.capacity());

        assertTrue(map.add(1));
        assertTrue(map.add(2));
        assertTrue(map.add(3));

        // expand hashmap
        assertEquals(8, map.capacity());

        assertTrue(map.remove(1));
        // not shrink
        assertEquals(8, map.capacity());
        assertTrue(map.remove(2));
        // shrink hashmap
        assertEquals(4, map.capacity());

        assertTrue(map.remove(3));
        // Will not shrink the hashmap again because shrink capacity is less than initCapacity
        // current capacity is equal than the initial capacity
        assertEquals(map.capacity(), initCapacity);
        map.clear();
        // after clear, because current capacity is equal than the initial capacity, so not shrinkToInitCapacity
        assertEquals(map.capacity(), initCapacity);
    }

    @Test
    public void testIteration() {
        ConcurrentLongHashSet set = ConcurrentLongHashSet.newBuilder().build();

        assertEquals(set.items(), Collections.emptySet());

        set.add(0L);

        assertEquals(set.items(), Sets.newHashSet(0L));

        set.remove(0L);

        assertEquals(set.items(), Collections.emptySet());

        set.add(0L);
        set.add(1L);
        set.add(2L);

        List<Long> values = Lists.newArrayList(set.items());
        Collections.sort(values);
        assertEquals(values, Lists.newArrayList(0L, 1L, 2L));

        set.clear();
        assertTrue(set.isEmpty());
    }

    @Test
    public void testHashConflictWithDeletion() {
        final int buckets = 16;
        ConcurrentLongHashSet set = ConcurrentLongHashSet.newBuilder()
                .expectedItems(buckets)
                .concurrencyLevel(1)
                .build();

        // Pick 2 keys that fall into the same bucket
        long key1 = 1;
        long key2 = 27;

        int bucket1 = ConcurrentOpenHashSet.signSafeMod(ConcurrentOpenHashSet.hash(key1), buckets);
        int bucket2 = ConcurrentOpenHashSet.signSafeMod(ConcurrentOpenHashSet.hash(key2), buckets);
        assertEquals(bucket1, bucket2);

        assertTrue(set.add(key1));
        assertTrue(set.add(key2));
        assertEquals(set.size(), 2);

        assertTrue(set.remove(key1));
        assertEquals(set.size(), 1);

        assertTrue(set.add(key1));
        assertEquals(set.size(), 2);

        assertTrue(set.remove(key1));
        assertEquals(set.size(), 1);

        assertFalse(set.add(key2));
        assertTrue(set.contains(key2));

        assertEquals(set.size(), 1);
        assertTrue(set.remove(key2));
        assertTrue(set.isEmpty());
    }

    @Test
    public void testSizeInBytes() {
        ConcurrentLongHashSet set = new ConcurrentLongHashSet(4, 2);
        assertEquals(64, set.sizeInBytes());
        set.add(1);
        assertEquals(64, set.sizeInBytes());
        set.add(2);
        assertEquals(64, set.sizeInBytes());
        set.add(3);
        assertEquals(64, set.sizeInBytes());
        set.add(4);
        assertEquals(96, set.sizeInBytes());
        set.add(5);
        assertEquals(96, set.sizeInBytes());
        set.add(6);
        assertEquals(128, set.sizeInBytes());
        set.add(7);
        assertEquals(128, set.sizeInBytes());
    }

}
