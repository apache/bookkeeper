/*
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
 */
package org.apache.bookkeeper.common.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Unit tests for {@link BlockingMpscQueue}.
 */
public class BlockingMpscQueueTest {

    @Test
    public void basicTest() throws Exception {
        final int size = 15;
        BlockingQueue<Integer> queue = new BlockingMpscQueue<>(size);

        for (int i = 0; i < size; i++) {
            queue.put(i);

            assertEquals(size - i, queue.remainingCapacity());
        }

        assertEquals(size, queue.size());

        for (int i = 0; i < size; i++) {
            Integer n = queue.take();
            assertTrue(n != null);
        }

        assertEquals(0, queue.size());

        Integer res = queue.poll(100, TimeUnit.MILLISECONDS);
        assertNull(res);
    }

    @Test
    public void testOffer() throws Exception {
        final int size = 16;
        BlockingQueue<Integer> queue = new BlockingMpscQueue<>(size);

        for (int i = 0; i < size; i++) {
            assertTrue(queue.offer(1, 100, TimeUnit.MILLISECONDS));
        }

        assertEquals(size, queue.size());

        assertFalse(queue.offer(1, 100, TimeUnit.MILLISECONDS));
        assertEquals(size, queue.size());
    }

    @Test
    public void testDrain() throws Exception {
        final int size = 10;
        BlockingQueue<Integer> queue = new BlockingMpscQueue<>(size);

        for (int i = 0; i < size; i++) {
            queue.put(i);
        }

        List<Integer> list = new ArrayList<>(size);
        queue.drainTo(list);

        assertEquals(size, list.size());

        assertEquals(0, queue.size());

        Integer res = queue.poll(100, TimeUnit.MILLISECONDS);
        assertNull(res);
    }

    @Test
    public void testDrainWithLimit() throws Exception {
        final int size = 10;
        BlockingQueue<Integer> queue = new BlockingMpscQueue<>(size);

        for (int i = 0; i < size; i++) {
            queue.put(i);
        }

        List<Integer> list = new ArrayList<>();
        queue.drainTo(list, 5);
        assertEquals(5, list.size());

        assertEquals(5, queue.size());
    }
}
