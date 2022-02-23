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

package org.apache.bookkeeper.common.util;

import com.google.common.primitives.Longs;

import java.security.SecureRandom;
import java.util.Random;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test how even is the distribution of ledgers across the threads of OrderedExecutor.
 */
@Slf4j
public class TestThreadSelection {

    public static final long MAX_KEY = 1_000_000L;
    public static final int MAX_THREADS = 96;
    public static final double MAX_DISPARITY = 1.25d;

    private final Random rnd = new SecureRandom();

    /**
     * Only even keys.
     */
    @Test
    public void testThreadSelectionEvenKeys() {
        runTest(0L, 2L);
    }

    /**
     * Only odd keys.
     */
    @Test
    public void testThreadSelectionOddKeys() {
        runTest(1L, 2L);
    }

    /**
     * All keys.
     */
    @Test
    public void testThreadSelectionAllKeys() {
        runTest(0L, 1L);
    }

    /**
     * Random keys.
     */
    @Test
    public void testThreadSelectionRandKeys() {
        for (int numThreads = 2; numThreads <= MAX_THREADS; numThreads++) {
            long[] placement = new long[numThreads];

            log.info("testing {} threads", numThreads);
            for (long key = 0L; key < MAX_KEY; key += 1L) {
                int threadId = OrderedExecutor.chooseThreadIdx(rnd.nextLong(), numThreads);
                placement[threadId]++;
            }
            validateTest(placement, numThreads);
        }
    }

    /**
     * Confirm the same key assigned to the same thread on consequent calls.
     */
    @Test
    public void testKeyAssignedToTheSameThread() {
        for (int numThreads = 2; numThreads <= MAX_THREADS; numThreads++) {

            log.info("testing {} threads", numThreads);
            for (long key = 0L; key < MAX_KEY; key += 1L) {
                int threadId = OrderedExecutor.chooseThreadIdx(key, numThreads);
                for (int i = 0; i < 10; i++) {
                    Assert.assertEquals("must be assigned to the same thread",
                            threadId, OrderedExecutor.chooseThreadIdx(key, numThreads));
                }
            }
        }
    }


    private void runTest(long start, long step) {
        for (int numThreads = 2; numThreads <= MAX_THREADS; numThreads++) {
            long[] placement = new long[numThreads];

            log.info("testing {} threads", numThreads);
            for (long key = start; key < MAX_KEY; key += step) {
                int threadId = OrderedExecutor.chooseThreadIdx(key, numThreads);
                placement[threadId]++;
            }
            validateTest(placement, numThreads);
        }
    }

    private void validateTest(long[] placement, int numThreads) {
        long min = Longs.min(placement);
        long max = Longs.max(placement);
        log.info("got min={}, max={} (disparity: {}) for {} threads with {} ids", min, max, numThreads, MAX_KEY);
        Assert.assertTrue("all threads were used [numThreads: " + numThreads + "]",
                min > 0);
        log.info("disparity = {}", String.format("%,.2f", (double) max / min));
        Assert.assertTrue("no large disparity found [numThreads: " + numThreads + "], got "
                        + (double) max / min,
                (double) max / min <= MAX_DISPARITY);
    }

}
