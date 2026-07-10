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
package org.apache.bookkeeper.common.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Test the growable batched array blocking queue.
 */
public class GrowableBatchedArrayBlockingQueueTest {

    @Test
    public void simple() throws Exception {
        GrowableBatchedArrayBlockingQueue<Integer> queue = new GrowableBatchedArrayBlockingQueue<>(4);

        assertNull(queue.poll());

        assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());

        try {
            queue.element();
            fail("Should have thrown exception");
        } catch (NoSuchElementException e) {
            // Expected
        }

        try {
            queue.iterator();
            fail("Should have thrown exception");
        } catch (UnsupportedOperationException e) {
            // Expected
        }

        // Test index rollover
        for (int i = 0; i < 100; i++) {
            queue.add(i);

            assertEquals(i, queue.take().intValue());
        }

        queue.offer(1);
        queue.offer(2);
        queue.offer(3);
        queue.offer(4);

        assertEquals(4, queue.size());

        List<Integer> list = new ArrayList<>();
        queue.drainTo(list, 3);

        assertEquals(1, queue.size());
        assertEquals(Lists.newArrayList(1, 2, 3), list);
        assertEquals(4, queue.peek().intValue());

        assertEquals(4, queue.element().intValue());
        assertEquals(4, queue.remove().intValue());
        try {
            queue.remove();
            fail("Should have thrown exception");
        } catch (NoSuchElementException e) {
            // Expected
        }
    }

    @Test
    public void blockingTake() throws Exception {
        GrowableBatchedArrayBlockingQueue<Integer> queue = new GrowableBatchedArrayBlockingQueue<>();

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                int expected = 0;

                for (int i = 0; i < 100; i++) {
                    int n = queue.take();

                    assertEquals(expected++, n);
                }

                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        int n = 0;
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                queue.put(n);
                ++n;
            }

            // Wait until all the entries are consumed
            while (!queue.isEmpty()) {
                Thread.sleep(1);
            }
        }

        latch.await();
    }

    @Test
    public void growArray() throws Exception {
        GrowableBatchedArrayBlockingQueue<Integer> queue = new GrowableBatchedArrayBlockingQueue<>(4);

        assertNull(queue.poll());

        assertTrue(queue.offer(1));
        assertTrue(queue.offer(2));
        assertTrue(queue.offer(3));
        assertTrue(queue.offer(4));
        assertTrue(queue.offer(5));

        assertEquals(5, queue.size());

        queue.clear();
        assertEquals(0, queue.size());

        assertTrue(queue.offer(1, 1, TimeUnit.SECONDS));
        assertTrue(queue.offer(2, 1, TimeUnit.SECONDS));
        assertTrue(queue.offer(3, 1, TimeUnit.SECONDS));
        assertEquals(3, queue.size());

        List<Integer> list = new ArrayList<>();
        queue.drainTo(list);
        assertEquals(0, queue.size());

        assertEquals(Lists.newArrayList(1, 2, 3), list);
    }

    @Test
    public void growWithWrappedIndexes() throws Exception {
        GrowableBatchedArrayBlockingQueue<Integer> queue = new GrowableBatchedArrayBlockingQueue<>(4);

        assertTrue(queue.offer(0));
        assertTrue(queue.offer(1));
        assertTrue(queue.offer(2));

        // Advance the consumer index so that the queue content wraps around the array
        assertEquals(0, queue.poll().intValue());
        assertEquals(1, queue.poll().intValue());

        assertTrue(queue.offer(3));
        assertTrue(queue.offer(4));
        assertTrue(queue.offer(5));

        // Next offer triggers the growth with wrapped content
        assertTrue(queue.offer(6));
        assertEquals(5, queue.size());

        for (int i = 2; i <= 6; i++) {
            assertEquals(i, queue.take().intValue());
        }
        assertEquals(0, queue.size());
    }

    @Test
    public void putAllAndTakeAll() throws Exception {
        GrowableBatchedArrayBlockingQueue<Integer> queue = new GrowableBatchedArrayBlockingQueue<>(4);

        Integer[] items = new Integer[100];
        for (int i = 0; i < 100; i++) {
            items[i] = i;
        }

        // Insert, in one shot, more items than the current capacity
        queue.putAll(items, 0, 100);
        assertEquals(100, queue.size());

        Integer[] local = new Integer[100];
        int n = queue.takeAll(local);
        assertEquals(100, n);
        for (int i = 0; i < 100; i++) {
            assertEquals(i, local[i].intValue());
        }
        assertEquals(0, queue.size());
    }

    @Test
    public void putAllWithWrappedIndexes() throws Exception {
        GrowableBatchedArrayBlockingQueue<Integer> queue = new GrowableBatchedArrayBlockingQueue<>(8);

        // Advance the indexes so that the batch insert wraps around the array
        for (int i = 0; i < 6; i++) {
            queue.put(i);
        }
        for (int i = 0; i < 6; i++) {
            assertEquals(i, queue.take().intValue());
        }

        Integer[] items = new Integer[4];
        for (int i = 0; i < 4; i++) {
            items[i] = 100 + i;
        }

        queue.putAll(items, 0, 4);
        assertEquals(4, queue.size());

        for (int i = 0; i < 4; i++) {
            assertEquals(100 + i, queue.take().intValue());
        }
    }

    @Test
    public void takeAllBlocksUntilItemsAreAvailable() throws Exception {
        GrowableBatchedArrayBlockingQueue<Integer> queue = new GrowableBatchedArrayBlockingQueue<>();

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                Integer[] local = new Integer[10];
                int n = queue.takeAll(local);
                assertEquals(1, n);
                assertEquals(1, local[0].intValue());

                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // Make sure the background thread is waiting on takeAll
        Thread.sleep(100);
        queue.put(1);

        latch.await();
    }

    @Test
    public void pollAllTimeout() throws Exception {
        GrowableBatchedArrayBlockingQueue<Integer> queue = new GrowableBatchedArrayBlockingQueue<>();

        Integer[] local = new Integer[10];
        assertEquals(0, queue.pollAll(local, 1, TimeUnit.MILLISECONDS));

        queue.put(1);
        queue.put(2);
        assertEquals(2, queue.pollAll(local, 1, TimeUnit.MILLISECONDS));
        assertEquals(1, local[0].intValue());
        assertEquals(2, local[1].intValue());
    }

    @Test
    public void pollTimeout() throws Exception {
        GrowableBatchedArrayBlockingQueue<Integer> queue = new GrowableBatchedArrayBlockingQueue<>(4);

        assertNull(queue.poll(1, TimeUnit.MILLISECONDS));

        queue.put(1);
        assertEquals(1, queue.poll(1, TimeUnit.MILLISECONDS).intValue());

        // 0 timeout should not block
        assertNull(queue.poll(0, TimeUnit.HOURS));

        queue.put(2);
        queue.put(3);
        assertEquals(2, queue.poll(1, TimeUnit.HOURS).intValue());
        assertEquals(3, queue.poll(1, TimeUnit.HOURS).intValue());
    }
}
