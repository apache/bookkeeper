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
public class GrowableArrayBlockingQueueTest {

    @Test
    public void simple() throws Exception {
        BlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>(4);

        assertEquals(null, queue.poll());

        assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
        assertEquals("[]", queue.toString());

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
        assertEquals("[1]", queue.toString());
        queue.offer(2);
        assertEquals("[1, 2]", queue.toString());
        queue.offer(3);
        assertEquals("[1, 2, 3]", queue.toString());
        queue.offer(4);
        assertEquals("[1, 2, 3, 4]", queue.toString());

        assertEquals(4, queue.size());

        List<Integer> list = new ArrayList<>();
        queue.drainTo(list, 3);

        assertEquals(1, queue.size());
        assertEquals(Lists.newArrayList(1, 2, 3), list);
        assertEquals("[4]", queue.toString());
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
        BlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>();

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
        BlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>(4);

        assertEquals(null, queue.poll());

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
    public void pollTimeout() throws Exception {
        BlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>(4);

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
        BlockingQueue<Integer> queue = new GrowableArrayBlockingQueue<>();

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
}
