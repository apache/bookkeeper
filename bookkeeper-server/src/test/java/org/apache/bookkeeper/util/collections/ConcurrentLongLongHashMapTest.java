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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap.LongLongFunction;
import org.junit.Test;

/**
 * Test the ConcurrentLongLongHashMap class.
 */
public class ConcurrentLongLongHashMapTest {

    @Test
    public void testConstructor() {
        try {
            ConcurrentLongLongHashMap.newBuilder()
                    .expectedItems(0)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentLongLongHashMap.newBuilder()
                    .expectedItems(16)
                    .concurrencyLevel(0)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            ConcurrentLongLongHashMap.newBuilder()
                    .expectedItems(4)
                    .concurrencyLevel(8)
                    .build();
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void simpleInsertions() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .build();

        assertTrue(map.isEmpty());
        assertEquals(map.put(1, 11), -1);
        assertFalse(map.isEmpty());

        assertEquals(map.put(2, 22), -1);
        assertEquals(map.put(3, 33), -1);

        assertEquals(map.size(), 3);

        assertEquals(map.get(1), 11);
        assertEquals(map.size(), 3);

        assertEquals(map.remove(1), 11);
        assertEquals(map.size(), 2);
        assertEquals(map.get(1), -1);
        assertEquals(map.get(5), -1);
        assertEquals(map.size(), 2);

        assertEquals(map.put(1, 11), -1);
        assertEquals(map.size(), 3);
        assertEquals(map.put(1, 111), 11);
        assertEquals(map.size(), 3);
    }

    @Test
    public void testClear() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertTrue(map.capacity() == 4);

        assertTrue(map.put(1, 1) == -1);
        assertTrue(map.put(2, 2) == -1);
        assertTrue(map.put(3, 3) == -1);

        assertTrue(map.capacity() == 8);
        map.clear();
        assertTrue(map.capacity() == 4);
    }

    @Test
    public void testExpandAndShrink() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(2)
                .concurrencyLevel(1)
                .autoShrink(true)
                .mapIdleFactor(0.25f)
                .build();
        assertTrue(map.put(1, 1) == -1);
        assertTrue(map.put(2, 2) == -1);
        assertTrue(map.put(3, 3) == -1);

        // expand hashmap
        assertTrue(map.capacity() == 8);

        assertTrue(map.remove(1, 1));
        // not shrink
        assertTrue(map.capacity() == 8);
        assertTrue(map.remove(2, 2));
        // shrink hashmap
        assertTrue(map.capacity() == 4);

        // expand hashmap
        assertTrue(map.put(4, 4) == -1);
        assertTrue(map.put(5, 5) == -1);
        assertTrue(map.capacity() == 8);

        //verify that the map does not keep shrinking at every remove() operation
        assertTrue(map.put(6, 6) == -1);
        assertTrue(map.remove(6, 6));
        assertTrue(map.capacity() == 8);
    }

    @Test
    public void testRemove() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder().build();

        assertTrue(map.isEmpty());
        assertEquals(map.put(1, 11), -1);
        assertFalse(map.isEmpty());

        assertFalse(map.remove(0, 0));
        assertFalse(map.remove(1, 111));

        assertFalse(map.isEmpty());
        assertTrue(map.remove(1, 11));
        assertTrue(map.isEmpty());
    }

    @Test
    public void testNegativeUsedBucketCount() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();

        map.put(0, 0);
        assertEquals(1, map.getUsedBucketCount());
        map.put(0, 1);
        assertEquals(1, map.getUsedBucketCount());
        map.remove(0);
        assertEquals(0, map.getUsedBucketCount());
        map.remove(0);
        assertEquals(0, map.getUsedBucketCount());
    }

    @Test
    public void testRehashing() {
        int n = 16;
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(n / 2)
                .concurrencyLevel(1)
                .build();
        assertEquals(map.capacity(), n);
        assertEquals(map.size(), 0);

        for (int i = 0; i < n; i++) {
            map.put(i, i);
        }

        assertEquals(map.capacity(), 2 * n);
        assertEquals(map.size(), n);
    }

    @Test
    public void testRehashingWithDeletes() {
        int n = 16;
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(n / 2)
                .concurrencyLevel(1)
                .build();
        assertEquals(map.capacity(), n);
        assertEquals(map.size(), 0);

        for (int i = 0; i < n / 2; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < n / 2; i++) {
            map.remove(i);
        }

        for (int i = n; i < (2 * n); i++) {
            map.put(i, i);
        }

        assertEquals(map.capacity(), 2 * n);
        assertEquals(map.size(), n);
    }

    @Test
    public void concurrentInsertions() throws Throwable {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder().build();
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int n = 100_000;
        long value = 55;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                Random random = new Random();

                for (int j = 0; j < n; j++) {
                    long key = Math.abs(random.nextLong());
                    // Ensure keys are uniques
                    key -= key % (threadIdx + 1);

                    map.put(key, value);
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
    public void concurrentInsertionsAndReads() throws Throwable {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder().build();
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int n = 100_000;
        final long value = 55;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                Random random = new Random();

                for (int j = 0; j < n; j++) {
                    long key = Math.abs(random.nextLong());
                    // Ensure keys are uniques
                    key -= key % (threadIdx + 1);

                    map.put(key, value);
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
    public void testIteration() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder().build();

        assertEquals(map.keys(), Collections.emptyList());
        assertEquals(map.values(), Collections.emptyList());

        map.put(0, 0);

        assertEquals(map.keys(), Lists.newArrayList(0L));
        assertEquals(map.values(), Lists.newArrayList(0L));

        map.remove(0);

        assertEquals(map.keys(), Collections.emptyList());
        assertEquals(map.values(), Collections.emptyList());

        map.put(0, 0);
        map.put(1, 11);
        map.put(2, 22);

        List<Long> keys = map.keys();
        Collections.sort(keys);
        assertEquals(keys, Lists.newArrayList(0L, 1L, 2L));

        List<Long> values = map.values();
        Collections.sort(values);
        assertEquals(values, Lists.newArrayList(0L, 11L, 22L));

        map.put(1, 111);

        keys = map.keys();
        Collections.sort(keys);
        assertEquals(keys, Lists.newArrayList(0L, 1L, 2L));

        values = map.values();
        Collections.sort(values);
        assertEquals(values, Lists.newArrayList(0L, 22L, 111L));

        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testHashConflictWithDeletion() {
        final int buckets = 16;
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(buckets)
                .concurrencyLevel(1)
                .build();

        // Pick 2 keys that fall into the same bucket
        long key1 = 1;
        long key2 = 27;

        int bucket1 = ConcurrentLongLongHashMap.signSafeMod(ConcurrentLongLongHashMap.hash(key1), buckets);
        int bucket2 = ConcurrentLongLongHashMap.signSafeMod(ConcurrentLongLongHashMap.hash(key2), buckets);
        assertEquals(bucket1, bucket2);

        final long value1 = 1;
        final long value2 = 2;
        final long value1Overwrite = 3;
        final long value2Overwrite = 3;

        assertEquals(map.put(key1, value1), -1);
        assertEquals(map.put(key2, value2), -1);
        assertEquals(map.size(), 2);

        assertEquals(map.remove(key1), value1);
        assertEquals(map.size(), 1);

        assertEquals(map.put(key1, value1Overwrite), -1);
        assertEquals(map.size(), 2);

        assertEquals(map.remove(key1), value1Overwrite);
        assertEquals(map.size(), 1);

        assertEquals(map.put(key2, value2Overwrite), value2);
        assertEquals(map.get(key2), value2Overwrite);

        assertEquals(map.size(), 1);
        assertEquals(map.remove(key2), value2Overwrite);
        assertTrue(map.isEmpty());
    }

    @Test
    public void testPutIfAbsent() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder().build();
        assertEquals(map.putIfAbsent(1, 11), -1);
        assertEquals(map.get(1), 11);

        assertEquals(map.putIfAbsent(1, 111), 11);
        assertEquals(map.get(1), 11);
    }

    @Test
    public void testComputeIfAbsent() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();
        AtomicLong counter = new AtomicLong();
        LongLongFunction provider = new LongLongFunction() {
            public long apply(long key) {
                return counter.getAndIncrement();
            }
        };

        assertEquals(map.computeIfAbsent(0, provider), 0);
        assertEquals(map.get(0), 0);

        assertEquals(map.computeIfAbsent(1, provider), 1);
        assertEquals(map.get(1), 1);

        assertEquals(map.computeIfAbsent(1, provider), 1);
        assertEquals(map.get(1), 1);

        assertEquals(map.computeIfAbsent(2, provider), 2);
        assertEquals(map.get(2), 2);
    }

    @Test
    public void testAddAndGet() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();

        assertEquals(map.addAndGet(0, 0), 0);
        assertEquals(map.containsKey(0), true);
        assertEquals(map.get(0), 0);

        assertEquals(map.containsKey(5), false);

        assertEquals(map.addAndGet(0, 5), 5);
        assertEquals(map.get(0), 5);

        assertEquals(map.addAndGet(0, 1), 6);
        assertEquals(map.get(0), 6);

        assertEquals(map.addAndGet(0, -2), 4);
        assertEquals(map.get(0), 4);

        // Cannot bring to value to negative
        try {
            map.addAndGet(0, -5);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
        assertEquals(map.get(0), 4);
    }

    @Test
    public void testRemoveIf() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();

        map.put(1L, 1L);
        map.put(2L, 2L);
        map.put(3L, 3L);
        map.put(4L, 4L);

        map.removeIf(key -> key < 3);
        assertFalse(map.containsKey(1L));
        assertFalse(map.containsKey(2L));
        assertTrue(map.containsKey(3L));
        assertTrue(map.containsKey(4L));
        assertEquals(2, map.size());
    }

    @Test
    public void testRemoveIfValue() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();

        map.put(1L, 1L);
        map.put(2L, 2L);
        map.put(3L, 1L);
        map.put(4L, 2L);

        map.removeIf((key, value) -> value < 2);
        assertFalse(map.containsKey(1L));
        assertTrue(map.containsKey(2L));
        assertFalse(map.containsKey(3L));
        assertTrue(map.containsKey(4L));
        assertEquals(2, map.size());
    }

    @Test
    public void testIvalidKeys() {
        ConcurrentLongLongHashMap map = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();

        try {
            map.put(-5, 4);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            map.get(-1);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            map.containsKey(-1);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            map.putIfAbsent(-1, 1);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            map.computeIfAbsent(-1, new LongLongFunction() {
                @Override
                public long apply(long key) {
                    return 1;
                }
            });
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void testAsMap() {
        ConcurrentLongLongHashMap lmap = ConcurrentLongLongHashMap.newBuilder()
                .expectedItems(16)
                .concurrencyLevel(1)
                .build();
        lmap.put(1, 11);
        lmap.put(2, 22);
        lmap.put(3, 33);

        Map<Long, Long> map = Maps.newTreeMap();
        map.put(1L, 11L);
        map.put(2L, 22L);
        map.put(3L, 33L);

        assertEquals(map, lmap.asMap());
    }
}
