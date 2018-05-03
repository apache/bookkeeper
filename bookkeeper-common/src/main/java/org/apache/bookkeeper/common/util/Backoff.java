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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import lombok.Data;
import lombok.ToString;
import org.apache.bookkeeper.common.util.Backoff.Jitter.Type;

/**
 * Implements various backoff strategies.
 *
 * <p>Strategies are defined by a {@link java.util.stream.Stream} of durations
 * and are intended to determine the duration after which a task is to be
 * retried.
 */
public class Backoff {

    public static final Policy DEFAULT = Jitter.of(
        Type.EXPONENTIAL,
        200,
        2000,
        3);

    private static final int MaxBitShift = 62;

    /**
     * Back off policy.
     *
     * <p>It defines a stream of time durations that will be used for backing off.
     */
    public interface Policy {

        Policy NONE = () -> Stream.empty();

        /**
         * Convert the policy into a series of backoff values.
         *
         * @return a series of backoff values.
         */
        Stream<Long> toBackoffs();

    }

    /**
     * A constant backoff policy.
     */
    @Data(staticConstructor = "of")
    @ToString
    public static class Constant implements Policy {

        /**
         * Create infinite constant backoff stream.
         *
         * <p>It is the infinite version of {@link #of(long, long)}.
         *
         * @param ms constant backoff time in milliseconds.
         * @return constant backoff policy.
         */
        public static Constant of(long ms) {
            return of(ms, -1);
        }

        private final long ms;
        private final long limit;

        @Override
        public Stream<Long> toBackoffs() {
            if (limit >= 0) {
                return constant(ms).limit(limit);
            } else {
                return constant(ms);
            }
        }
    }

    /**
     * A Jittered backoff policy.
     *
     * <p>It is an implementation of {@link http://www.awsarchitectureblog.com/2015/03/backoff.html}
     */
    @Data(staticConstructor = "of")
    @ToString
    public static class Jitter implements Policy {

        /**
         * Jitter type.
         */
        public enum Type {
            DECORRELATED,
            EQUAL,
            EXPONENTIAL
        }

        /**
         * Create infinite jittered backoff stream.
         *
         * <p>It is the infinite version of {@link #of(Type, long, long, long)}.
         *
         * @param type    jittered backoff type
         * @param startMs the start backoff time in milliseconds.
         * @param maxMs   the max backoff time in milliseconds.
         * @return jittered backoff policy.
         */
        public static Jitter of(Type type, long startMs, long maxMs) {
            return of(type, startMs, maxMs, -1);
        }

        private final Type type;
        private final long startMs;
        private final long maxMs;
        private final long limit;

        @Override
        public Stream<Long> toBackoffs() {
            Stream<Long> backoffStream;
            switch (type) {
                case DECORRELATED:
                    backoffStream = decorrelatedJittered(startMs, maxMs);
                    break;
                case EQUAL:
                    backoffStream = equalJittered(startMs, maxMs);
                    break;
                case EXPONENTIAL:
                default:
                    backoffStream = exponentialJittered(startMs, maxMs);
                    break;
            }
            if (limit >= 0) {
                return backoffStream.limit(limit);
            } else {
                return backoffStream;
            }
        }
    }

    /**
     * A exponential backoff policy.
     */
    @Data(staticConstructor = "of")
    @ToString
    public static class Exponential implements Policy {

        /**
         * Create an infinite exponential backoff policy.
         *
         * <p>It is the infinite version of {@link #of(long, long, int, int)}.
         *
         * @param startMs    start backoff time in milliseconds.
         * @param maxMs      max backoff time in milliseconds.
         * @param multiplier the backoff multiplier
         * @return the exponential backoff policy.
         */
        public static Exponential of(long startMs, long maxMs, int multiplier) {
            return of(startMs, maxMs, multiplier, -1);
        }

        private final long startMs;
        private final long maxMs;
        private final int multiplier;
        private final int limit;

        @Override
        public Stream<Long> toBackoffs() {
            if (limit >= 0) {
                return exponential(startMs, multiplier, maxMs).limit(limit);
            } else {
                return exponential(startMs, multiplier, maxMs);
            }
        }
    }

    /**
     * Create a stream with constant backoffs.
     *
     * @param startMs initial backoff in milliseconds
     * @return a stream with constant backoff values.
     */
    public static Stream<Long> constant(long startMs) {
        return Stream.iterate(startMs, lastMs -> startMs);
    }

    /**
     * Create a stream with exponential backoffs.
     *
     * @param startMs    initial backoff in milliseconds.
     * @param multiplier the multiplier for next backoff.
     * @param maxMs      max backoff in milliseconds.
     * @return a stream with exponential backoffs.
     */
    public static Stream<Long> exponential(long startMs,
                                           int multiplier,
                                           long maxMs) {
        return Stream.iterate(startMs, lastMs -> Math.min(lastMs * multiplier, maxMs));
    }

    /**
     * Create a stream of exponential backoffs with jitters.
     *
     * <p>This is "full jitter" via http://www.awsarchitectureblog.com/2015/03/backoff.html
     *
     * @param startMs initial backoff in milliseconds.
     * @param maxMs   max backoff in milliseconds.
     * @return a stream of exponential backoffs with jitters.
     */
    public static Stream<Long> exponentialJittered(long startMs,
                                                   long maxMs) {
        final long startNanos = TimeUnit.NANOSECONDS.convert(startMs, TimeUnit.MILLISECONDS);
        final long maxNanos = TimeUnit.NANOSECONDS.convert(maxMs, TimeUnit.MILLISECONDS);
        final AtomicLong attempts = new AtomicLong(1);
        return Stream.iterate(startMs, lastMs -> {
            long shift = Math.min(attempts.get(), MaxBitShift);
            long maxBackoffNanos = Math.min(maxNanos, startNanos * (1L << shift));
            long randomMs = TimeUnit.MILLISECONDS.convert(
                ThreadLocalRandom.current().nextLong(startNanos, maxBackoffNanos),
                TimeUnit.NANOSECONDS);
            attempts.incrementAndGet();
            return randomMs;
        });
    }

    /**
     * Create an infinite backoffs that have jitter with a random distribution
     * between {@code startMs} and 3 times the previously selected value, capped at {@code maxMs}.
     *
     * <p>this is "decorrelated jitter" via http://www.awsarchitectureblog.com/2015/03/backoff.html
     *
     * @param startMs initial backoff in milliseconds
     * @param maxMs   max backoff in milliseconds
     * @return a stream of jitter backoffs.
     */
    public static Stream<Long> decorrelatedJittered(long startMs,
                                                    long maxMs) {
        final long startNanos = TimeUnit.NANOSECONDS.convert(startMs, TimeUnit.MILLISECONDS);
        final long maxNanos = TimeUnit.NANOSECONDS.convert(maxMs, TimeUnit.MILLISECONDS);
        return Stream.iterate(startMs, lastMs -> {
            long lastNanos = TimeUnit.MILLISECONDS.convert(lastMs, TimeUnit.NANOSECONDS);
            long randRange = Math.abs(lastNanos * 3 - startNanos);
            long randBackoff;
            if (0L == randRange) {
                randBackoff = startNanos;
            } else {
                randBackoff = startNanos + ThreadLocalRandom.current().nextLong(randRange);
            }
            long backOffNanos = Math.min(maxNanos, randBackoff);
            return TimeUnit.MILLISECONDS.convert(backOffNanos, TimeUnit.NANOSECONDS);
        });

    }

    /**
     * Create infinite backoffs that keep half of the exponential growth, and jitter
     * between 0 and that amount.
     *
     * <p>this is "equal jitter" via http://www.awsarchitectureblog.com/2015/03/backoff.html
     *
     * @param startMs initial backoff in milliseconds.
     * @param maxMs   max backoff in milliseconds.
     * @return a stream of exponential backoffs with jitters.
     */
    public static Stream<Long> equalJittered(long startMs,
                                             long maxMs) {
        final long startNanos = TimeUnit.NANOSECONDS.convert(startMs, TimeUnit.MILLISECONDS);
        final long maxNanos = TimeUnit.NANOSECONDS.convert(maxMs, TimeUnit.MILLISECONDS);
        final AtomicLong attempts = new AtomicLong(1);
        return Stream.iterate(startMs, lastMs -> {
            long shift = Math.min(attempts.get() - 1, MaxBitShift);
            long halfExpNanos = startNanos * (1L << shift);
            long backoffNanos = halfExpNanos + ThreadLocalRandom.current().nextLong(halfExpNanos);
            attempts.incrementAndGet();
            if (backoffNanos < maxNanos) {
                return TimeUnit.MILLISECONDS.convert(backoffNanos, TimeUnit.NANOSECONDS);
            } else {
                return maxMs;
            }
        });
    }

}
