/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.stats.codahale;

import static org.junit.Assert.assertEquals;

import com.codahale.metrics.Snapshot;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

/**
 * Unit tests for FastTimer.
 *
 */
public class FastTimerTest {

    /*
     * To simplify testing, we're over-riding the time source used by FastTimer with some
     * fake time we're incrementing manually. This speeds-up testing (we don't have to wait
     * for real seconds to elapse) and also guarantees deterministic behavior for the unit
     * test.
     */
    private static AtomicInteger mockedTime = new AtomicInteger(0);

    private int incSec() {
        return mockedTime.incrementAndGet();
    }

    private FastTimer getMockedFastTimer(int timeWindowSeconds, FastTimer.Buckets buckets) {
        return new FastTimer(timeWindowSeconds, buckets) {
            @Override
            protected int getTime() {
                return mockedTime.get();
            }
        };
    }

    @Test
    public void testBuckets() {
        FastTimer t = new FastTimer(1, FastTimer.Buckets.fine);
        for (int b = 0; b < t.getNumberOfBuckets(); b++) {
            long lowerBound = b > 0 ? t.getBucketBound(b - 1) + 1 : 0;
            long bucketMean = t.getBucketValue(b);
            long upperBound = t.getBucketBound(b);
            System.out.println(String.format("Bucket %3d [%12d - %12d], avg=%12d",
                    b, lowerBound, upperBound, bucketMean));
            assertEquals(String.format("bucket for lowerBound value %d", lowerBound),
                    b, t.getBucket(lowerBound));
            assertEquals(String.format("bucket for bucketMean value %d", bucketMean),
                    b, t.getBucket(bucketMean));
            assertEquals(String.format("bucket for upperBound value %d", upperBound),
                    b, t.getBucket(upperBound));
            if (b > 0) {
                assertEquals(String.format("bucket before bucket %d", b), b - 1, t.getBucket(lowerBound - 1));
            }
            if (b + 1 < t.getNumberOfBuckets()) {
                assertEquals(String.format("bucket after bucket %d", b), b + 1, t.getBucket(upperBound + 1));
            }
        }
    }

    @Test
    public void testFunctional() {
        FastTimer t = getMockedFastTimer(1, FastTimer.Buckets.fine);
        for (int i = 0; i <= 10000; i++) {
            t.update(i, TimeUnit.MICROSECONDS);
        }
        incSec(); // advance mocked time to next second
        Snapshot s = t.getSnapshot();
        assertEquals("FastTimer.getCount()", 10001, t.getCount());
        assertEquals("FastSnapshot.getMin()", 1, s.getMin());
        assertEquals("FastSnapshot.getMax()", TimeUnit.MICROSECONDS.toNanos(10000), s.getMax());
        assertEquals("FastSnapshot.getMean()", TimeUnit.MICROSECONDS.toNanos(5000), (long) s.getMean());
        assertEquals("FastSnapshot.getMedian()", TimeUnit.MICROSECONDS.toNanos(5000), (long) s.getMedian());
        assertEquals("FastSnapshot.getValue(0.1)", TimeUnit.MICROSECONDS.toNanos(1000), (long) s.getValue(0.1));
        assertEquals("FastSnapshot.getValue(0.9)", TimeUnit.MICROSECONDS.toNanos(9000), (long) s.getValue(0.9));
        assertEquals("FastSnapshot.getValue(0.99)", TimeUnit.MICROSECONDS.toNanos(9900), (long) s.getValue(0.99));
    }

    @Test
    public void testTimer() {
        // load definitions for testing the timer
        // following 3 array lengths must match: each element defines values for one phase
        final int[] timeRange = new int[] {   90,  190,   50,   90, 100, 100 };
        final int[] timeBase = new int[]  {   10,   10,   50,   10,   0,   0 };
        final int[] rate = new int[]      { 1000, 1000, 1000, 1000,   0,   1 };

        final int window = 5; // use a 5 second window for testing
        FastTimer t = getMockedFastTimer(window, FastTimer.Buckets.fine);
        Random r = new Random(12345); // fixed random seed for deterministic value distribution
        int phase = 0;
        int sec = 0;

        long count = 0;
        // start generating test load for each of the configured phases
        while (phase < timeRange.length) {
            for (int i = 0; i < rate[phase]; i++) {
                t.update(r.nextInt(timeRange[phase]) + timeBase[phase], TimeUnit.MILLISECONDS);
                count++;
            }
            incSec(); // advance mocked time to next second
            if (++sec % window == 0) {
                // every WINDOW seconds, check the timer values
                Snapshot s = t.getSnapshot();
                System.out.println(String.format(
                        "phase %3d: count=%10d, rate=%6.0f, min=%6.1f, avg=%6.1f, q99=%6.1f, max=%6.1f",
                        phase, t.getCount(), t.getMeanRate(), ((double) s.getMin()) / 1000000.0,
                        s.getMean() / 1000000.0, s.getValue(0.99) / 1000000.0, ((double) s.getMax()) / 1000000.0));

                // check count (events the timer has ever seen)
                assertEquals("FastTimer.getCount()", count, t.getCount());
                // check rate (should be precisely the configured rate)
                assertEquals("FastTimer.getMeanRate()", rate[phase],
                        (int) Math.round(t.getMeanRate()));
                assertEquals("FastTimer.getOneMinuteRate()", rate[phase],
                        (int) Math.round(t.getOneMinuteRate()));
                assertEquals("FastTimer.getFiveMinuteRate()", rate[phase],
                        (int) Math.round(t.getFiveMinuteRate()));
                assertEquals("FastTimer.getFifteenMinuteRate()", rate[phase],
                        (int) Math.round(t.getFifteenMinuteRate()));
                // at rates > 1000 (with fixed seed), we know that the following checks will be successful
                if (t.getMeanRate() >= 1000) {
                    // check minimum value == lower bound
                    assertEquals("FastSnapshot.getMin()", timeBase[phase], s.getMin() / 1000000);
                    // check maximum value == upper bound
                    assertEquals("FastSnapshot.getMax()", timeBase[phase] + timeRange[phase] - 1,
                            (s.getMax() / 1000000));
                    // check 99th percentile == upper bound
                    assertEquals("FastSnapshot.getValue(0.99)",
                            t.getBucketBound(t.getBucket(
                                    TimeUnit.MILLISECONDS.toNanos(timeBase[phase] + timeRange[phase] - 1))),
                            (long) s.getValue(0.99));
                    // check mean is within 10% of configured mean
                    assertEquals("FastSnapshot.getMean()", (timeBase[phase] + (timeRange[phase] / 2)) / 10,
                            (int) (Math.round(s.getMean() / 1000000) / 10));
                }

                // start next phase
                phase++;
            }
        }
    }

    @Test
    public void testTimerMultiThreaded() {
        final int window = 5; // use a 5 second window for testing
        FastTimer t = getMockedFastTimer(window, FastTimer.Buckets.fine);

        // start 10 threads, which each update the timer 1000 times
        ArrayList<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    t.update(10, TimeUnit.MILLISECONDS);
                }
            });
            threads.add(thread);
            thread.start();
        }
        // wait for 10 threads to finish
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        incSec(); // advance mocked time to next second

        assertEquals("FastTimer.getCount()", 10000, t.getCount());
        assertEquals("FastTimer.getMeanRate()", 2000, (int) Math.round(t.getMeanRate()));

        Snapshot s = t.getSnapshot();
        assertEquals("FastSnapshot.getMin()", 10, s.getMin() / 1000000);
        assertEquals("FastSnapshot.getMax()", 10, (s.getMax() / 1000000));
        assertEquals("FastSnapshot.getValue(0.99)", 10, Math.round(s.getValue(0.99) / 1000000));
        assertEquals("FastSnapshot.getMean()", 10, (int) Math.round(s.getMean() / 1000000));
    }

    @Test
    public void testTimerNoBuckets() {
        final int window = 5; // use a 5 second window for testing
        FastTimer t = getMockedFastTimer(window, FastTimer.Buckets.none);

        for (int i = 0; i < 1000; i++) {
            t.update(10, TimeUnit.MILLISECONDS);
        }
        incSec(); // advance mocked time to next second

        assertEquals("FastTimer.getCount()", 1000, t.getCount());
        assertEquals("FastTimer.getMeanRate()", 200, (int) Math.round(t.getMeanRate()));

        Snapshot s = t.getSnapshot();
        assertEquals("FastSnapshot.getMin()", 10, s.getMin() / 1000000);
        assertEquals("FastSnapshot.getMax()", 10, (s.getMax() / 1000000));
        assertEquals("FastSnapshot.getValue(0.99)", 0, Math.round(s.getValue(0.99) / 1000000));
        assertEquals("FastSnapshot.getMean()", 10, (int) Math.round(s.getMean() / 1000000));
    }

    @Test
    public void testSnapshotOutOfSync() {
        FastTimer t = getMockedFastTimer(1, FastTimer.Buckets.fine);
        t.update(t.getBucketBound(0) - 1, TimeUnit.NANOSECONDS); // add value to 1st bucket
        t.update(t.getBucketBound(1) - 1, TimeUnit.NANOSECONDS); // add value to 2nd bucket
        t.update(t.getBucketBound(2) - 1, TimeUnit.NANOSECONDS); // add value to 3rd bucket
        incSec(); // advance mocked time to next second
        Snapshot s1 = t.getSnapshot();
        long[] buckets = new long[t.getNumberOfBuckets()];
        buckets[0] = 1;
        buckets[1] = 1;
        buckets[2] = 1;
        Snapshot s2 = new FastSnapshot(t,
                t.getBucketBound(0) - 1,
                t.getBucketBound(2) - 1,
                t.getBucketBound(0) + t.getBucketBound(1) + t.getBucketBound(2) + 3,
                4, // count (4) is out of sync with number of recorded events in buckets (3)
                buckets);
        assertEquals("FastSnapshot.getMin()", s1.getMin(), s2.getMin());
        assertEquals("FastSnapshot.getMax()", s1.getMax(), s2.getMax());
        assertEquals("FastSnapshot.getValue(0.95)", (long) s1.getValue(0.95), (long) s2.getValue(0.95));
    }

}
