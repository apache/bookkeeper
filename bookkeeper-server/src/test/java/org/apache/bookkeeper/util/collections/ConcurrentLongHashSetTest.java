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
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

/**
 * Test the ConcurrentLongHashSet class.
 */
public class ConcurrentLongHashSetTest {

    @Test
    public void testConstructor() {
        try {
            new ConcurrentLongHashSet(0);
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            new ConcurrentLongHashSet(16, 0);
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            new ConcurrentLongHashSet(4, 8);
            fail("should have thrown exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void simpleInsertions() {
        ConcurrentLongHashSet set = new ConcurrentLongHashSet(16);

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
    public void testRemove() {
        ConcurrentLongHashSet set = new ConcurrentLongHashSet();

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
        ConcurrentLongHashSet set = new ConcurrentLongHashSet(n / 2, 1);
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
        ConcurrentLongHashSet set = new ConcurrentLongHashSet(n / 2, 1);
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
        ConcurrentLongHashSet set = new ConcurrentLongHashSet();
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int n = 100_000;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                Random random = new Random();

                for (int j = 0; j < n; j++) {
                    long key = Math.abs(random.nextLong());
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
        ConcurrentLongHashSet map = new ConcurrentLongHashSet();
        ExecutorService executor = Executors.newCachedThreadPool();

        final int nThreads = 16;
        final int n = 100_000;

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nThreads; i++) {
            final int threadIdx = i;

            futures.add(executor.submit(() -> {
                Random random = new Random();

                for (int j = 0; j < n; j++) {
                    long key = Math.abs(random.nextLong());
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
    public void testIteration() {
        ConcurrentLongHashSet set = new ConcurrentLongHashSet();

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
        ConcurrentLongHashSet set = new ConcurrentLongHashSet(buckets, 1);

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

}
