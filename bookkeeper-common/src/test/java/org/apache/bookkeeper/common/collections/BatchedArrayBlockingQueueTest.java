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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Test the growable array blocking queue.
 */
public class BatchedArrayBlockingQueueTest {

    @Test
    public void simple() throws Exception {
        BlockingQueue<Integer> queue = new BatchedArrayBlockingQueue<>(4);

        assertEquals(null, queue.poll());

        assertEquals(4, queue.remainingCapacity());

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
        BlockingQueue<Integer> queue = new GrowableMpScArrayConsumerBlockingQueue<>();

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
    public void blockWhenFull() throws Exception {
        BlockingQueue<Integer> queue = new BatchedArrayBlockingQueue<>(4);

        assertEquals(null, queue.poll());

        assertTrue(queue.offer(1));
        assertTrue(queue.offer(2));
        assertTrue(queue.offer(3));
        assertTrue(queue.offer(4));
        assertFalse(queue.offer(5));

        assertEquals(4, queue.size());

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                queue.put(5);
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(100);
        assertEquals(1, latch.getCount());

        assertEquals(1, (int) queue.poll());

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(4, queue.size());


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
    public void pollTimeout() throws Exception {
        BlockingQueue<Integer> queue = new BatchedArrayBlockingQueue<>(4);

        assertEquals(null, queue.poll(1, TimeUnit.MILLISECONDS));

        queue.put(1);
        assertEquals(1, queue.poll(1, TimeUnit.MILLISECONDS).intValue());

        // 0 timeout should not block
        assertEquals(null, queue.poll(0, TimeUnit.HOURS));

        queue.put(2);
        queue.put(3);
        assertEquals(2, queue.poll(1, TimeUnit.HOURS).intValue());
        assertEquals(3, queue.poll(1, TimeUnit.HOURS).intValue());
    }

    @Test
    public void pollTimeout2() throws Exception {
        BlockingQueue<Integer> queue = new BatchedArrayBlockingQueue<>(10);

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                queue.poll(1, TimeUnit.HOURS);

                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // Make sure background thread is waiting on poll
        Thread.sleep(100);
        queue.put(1);

        latch.await();
    }


    @Test
    public void drainToArray() throws Exception {
        BatchedArrayBlockingQueue<Integer> queue = new BatchedArrayBlockingQueue<>(100);

        for (int i = 0; i < 10; i++) {
            queue.add(i);
        }

        Integer[] local = new Integer[5];
        int items = queue.takeAll(local);
        assertEquals(5, items);
        for (int i = 0; i < items; i++) {
            assertEquals(i, (int) local[i]);
        }

        assertEquals(5, queue.size());

        items = queue.takeAll(local);
        assertEquals(5, items);
        for (int i = 0; i < items; i++) {
            assertEquals(i + 5, (int) local[i]);
        }

        assertEquals(0, queue.size());

        /// Block when empty
        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                int c = queue.takeAll(local);
                assertEquals(1, c);
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(100);
        assertEquals(1, latch.getCount());

        assertEquals(0, queue.size());

        // Unblock the drain
        queue.put(1);

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(0, queue.size());
    }

    @Test
    public void putAll() throws Exception {
        BatchedArrayBlockingQueue<Integer> queue = new BatchedArrayBlockingQueue<>(10);

        Integer[] items = new Integer[100];
        for (int i = 0; i < 100; i++) {
            items[i] = i;
        }

        queue.putAll(items, 0, 5);
        assertEquals(5, queue.size());
        queue.putAll(items, 0, 5);
        assertEquals(10, queue.size());

        queue.clear();

        /// Block when empty
        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                queue.putAll(items, 0, 11);
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(100);
        assertEquals(1, latch.getCount());
        assertEquals(10, queue.size());

        // Unblock the putAll
        queue.take();

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(10, queue.size());
    }
}
