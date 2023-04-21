/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.util;

import static org.apache.bookkeeper.common.util.Backoff.Jitter.Type.DECORRELATED;
import static org.apache.bookkeeper.common.util.Backoff.Jitter.Type.EQUAL;
import static org.apache.bookkeeper.common.util.Backoff.Jitter.Type.EXPONENTIAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

/**
 * Unit test for {@link Backoff}.
 */
public class TestBackoff {

    static <T> void assertStreamEquals(Stream<T> s1, Stream<T> s2) {
        Iterator<T> iter1 = s1.iterator(), iter2 = s2.iterator();
        while (iter1.hasNext() && iter2.hasNext()) {
            T expectedValue = iter1.next();
            T actualValue = iter2.next();
            assertEquals("Expected = " + expectedValue + ", Actual = " + actualValue,
                expectedValue, actualValue);
        }
        assertTrue(!iter1.hasNext() && !iter2.hasNext());
    }

    @Test
    public void testExponential() throws Exception {
        Stream<Long> backoffs = Backoff.exponential(1000, 2, Long.MAX_VALUE).limit(10);
        Stream<Long> expectedBackoffs = LongStream.range(0L, 10L).mapToObj(i -> (1000L << i));
        assertStreamEquals(expectedBackoffs, backoffs);
    }

    @Test
    public void testExponentialPolicy() throws Exception {
        Stream<Long> expectedBackoffs = LongStream.range(0L, 10L).mapToObj(i -> (1000L << i));
        Backoff.Policy policy = Backoff.Exponential.of(1000, Long.MAX_VALUE, 2, 10);
        assertStreamEquals(expectedBackoffs, policy.toBackoffs());
    }

    @Test
    public void testExponentialWithUpperLimit() throws Exception {
        Stream<Long> backoffs = Backoff.exponential(1000, 2, 32000).limit(10);
        Stream<Long> expectedBackoffs = LongStream.range(0L, 10L).mapToObj(i -> Math.min(1000L << i, 32000));
        assertStreamEquals(expectedBackoffs, backoffs);
    }

    @Test
    public void testExponentialPolicyWithUpperLimit() throws Exception {
        Stream<Long> expectedBackoffs = LongStream.range(0L, 10L).mapToObj(i -> Math.min(1000L << i, 32000));
        Backoff.Policy policy = Backoff.Exponential.of(1000, 32000, 2, 10);
        assertStreamEquals(expectedBackoffs, policy.toBackoffs());
    }

    @Test
    public void testExponentialJittered() throws Exception {
        Stream<Long> backoffs = Backoff.exponentialJittered(5, 120).limit(10);
        // Expected: 5, then randos up to: 10, 20, 40, 80, 120, 120, 120 ...
        Stream<Long> maxBackoffs = Stream.of(5L, 10L, 20L, 40L, 80L, 120L, 120L, 120L, 120L, 120L);
        StreamUtil.<Long, Long, Void>zip(backoffs, maxBackoffs, (expected, actual) -> {
            assertTrue(expected <= actual);
            return null;
        });
    }

    @Test
    public void testExponentialJitteredPolicy() throws Exception {
        Stream<Long> backoffs = Backoff.Jitter.of(EXPONENTIAL, 5, 120, 10).toBackoffs();
        // Expected: 5, then randos up to: 10, 20, 40, 80, 120, 120, 120 ...
        Stream<Long> maxBackoffs = Stream.of(5L, 10L, 20L, 40L, 80L, 120L, 120L, 120L, 120L, 120L);
        StreamUtil.<Long, Long, Void>zip(backoffs, maxBackoffs, (expected, actual) -> {
            assertTrue(expected <= actual);
            return null;
        });
    }

    @Test
    public void testConstant() throws Exception {
        Stream<Long> backoffs = Backoff.constant(12345L).limit(10);
        Stream<Long> expectedBackoffs = LongStream.range(0L, 10L).mapToObj(i -> 12345L);
        assertStreamEquals(expectedBackoffs, backoffs);
    }

    @Test
    public void testConstantPolicy() throws Exception {
        Stream<Long> backoffs = Backoff.Constant.of(12345L, 10).toBackoffs();
        Stream<Long> expectedBackoffs = LongStream.range(0L, 10L).mapToObj(i -> 12345L);
        assertStreamEquals(expectedBackoffs, backoffs);
    }

    @Test
    public void testEqualJittered() throws Exception {
        Stream<Long> backoffs = Backoff.equalJittered(5, 120).limit(10);
        Stream<Pair<Long, Long>> ranges = Stream.of(
            Pair.of(5L, 10L),
            Pair.of(10L, 20L),
            Pair.of(20L, 40L),
            Pair.of(40L, 80L),
            Pair.of(80L, 120L),
            Pair.of(80L, 120L),
            Pair.of(80L, 120L),
            Pair.of(80L, 120L),
            Pair.of(80L, 120L)
        );
        StreamUtil.<Long, Pair<Long, Long>, Void>zip(backoffs, ranges, (backoff, maxPair) -> {
            assertTrue(backoff >= maxPair.getLeft());
            assertTrue(backoff <= maxPair.getRight());
            return null;
        });
    }

    @Test
    public void testEqualJitteredPolicy() throws Exception {
        Stream<Long> backoffs = Backoff.Jitter.of(EQUAL, 5, 120, 10).toBackoffs();
        Stream<Pair<Long, Long>> ranges = Stream.of(
            Pair.of(5L, 10L),
            Pair.of(10L, 20L),
            Pair.of(20L, 40L),
            Pair.of(40L, 80L),
            Pair.of(80L, 120L),
            Pair.of(80L, 120L),
            Pair.of(80L, 120L),
            Pair.of(80L, 120L),
            Pair.of(80L, 120L)
        );
        StreamUtil.<Long, Pair<Long, Long>, Void>zip(backoffs, ranges, (backoff, maxPair) -> {
            assertTrue(backoff >= maxPair.getLeft());
            assertTrue(backoff <= maxPair.getRight());
            return null;
        });
    }

    @Test
    public void testDecorrelatedJittered() throws Exception {
        long startMs = ThreadLocalRandom.current().nextLong(1L, 1000L);
        long maxMs = ThreadLocalRandom.current().nextLong(startMs, startMs * 2);
        Stream<Long> backoffs = Backoff.decorrelatedJittered(startMs, maxMs).limit(10);
        Iterator<Long> backoffIter = backoffs.iterator();
        assertTrue(backoffIter.hasNext());
        assertEquals(startMs, backoffIter.next().longValue());
        AtomicLong prevMs = new AtomicLong(startMs);
        backoffIter.forEachRemaining(backoffMs -> {
            assertTrue(backoffMs >= startMs);
            assertTrue(backoffMs <= prevMs.get() * 3);
            assertTrue(backoffMs <= maxMs);
            prevMs.set(backoffMs);
        });
    }

    @Test
    public void testDecorrelatedJitteredPolicy() throws Exception {
        long startMs = ThreadLocalRandom.current().nextLong(1L, 1000L);
        long maxMs = ThreadLocalRandom.current().nextLong(startMs, startMs * 2);
        Stream<Long> backoffs = Backoff.Jitter.of(DECORRELATED, startMs, maxMs, 10).toBackoffs();
        Iterator<Long> backoffIter = backoffs.iterator();
        assertTrue(backoffIter.hasNext());
        assertEquals(startMs, backoffIter.next().longValue());
        AtomicLong prevMs = new AtomicLong(startMs);
        backoffIter.forEachRemaining(backoffMs -> {
            assertTrue(backoffMs >= startMs);
            assertTrue(backoffMs <= prevMs.get() * 3);
            assertTrue(backoffMs <= maxMs);
            prevMs.set(backoffMs);
        });
    }

}
