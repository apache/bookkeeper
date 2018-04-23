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

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A fast and (nearly) garbage-free Rate and Response Times Timer.
 * FastTimer uses circular arrays which are allocated upfront.
 * Timer updates or queries never allocate new objects and thus never
 * create garbage.
 * A small number of new objects are allocated for snapshots when
 * calling getSnapshot().
 */
public class FastTimer extends Timer {

    /*
     * Design Considerations
     * ---------------------
     *
     * The design goals of this timer implementation are for it to be
     *  - fast (i.e. few instructions to update a timer)
     *  - scalable (i.e. little synchronization cost for concurrent timer updates)
     *  - garbage-free for timer updates (i.e. no object allocation for timer updates)
     *  - space-efficient (i.e. as little memory footprint as possible while achieving first three goals)
     *  - provide similar functionality as Codahale's default timers with ExponentiallyDecayingReservoirs
     *
     * This implementation provides rate and response times over a configurable sliding time window. Data
     * is stored in upfront allocated circular arrays, in which each array element holds data
     * for one second. Data is overwritten in a circular fashion without the allocation of new data
     * structures and is therefore garbage-free for all timer updates.
     *
     * This implementation does not store individual response times, but instead allocates bucketized counters
     * upfront, which are incremented for any event falling into a particular response time bucket. A
     * fine-grained bucket definition (intended for capturing successsful events) and a coarse-grained
     * bucket definition (intended to capture failure or timed-out events) are provided.
     *
     * To improve scalability of concurrent timer updates, most data structures are replicated HASH_SIZE
     * times, and calling threads updating a timer are hashed to individual instances. Performance tests
     * (see below) have shown that this implementation is light-weight enough to achieve slightly better
     * scalability than Codahale's default timers even without hashing, and can further improve scalability
     * if hashing is used.
     *
     * Trading off performance and scalability vs. memory footprint, we need to be conservative in the hash
     * size we chose. Different flavors of this timer implementation have been evaluated using JMH
     * micro-benchmarks (see microbenchmarks/src/main/java/org/apache/bookkeeper/stats/TimerBenchmark.java),
     * comparing implementations of FastTimer with a time window of 60 seconds and
     * - (DEV1)   a HASH_SIZE of 3 for all data structures (meters, counters, min/max, and response time buckets)
     * - (DEV2)   a HASH_SIZE of 1 for all data structures (meters, counters, min/max, and response time buckets)
     * - (FINAL)  a HASH_SIZE of 3 for meters, counters, min/max, and no hashing for response time buckets
     * to the default timer implementation
     * - (BASE-E) Codahale Timer with ExponentiallyDecayingReservoir (default as used by bookkeeper code)
     * - (BASE-T) Codahale Timer with SlidingTimeWindowReservoir configured to 60 seconds
     * - (BASE-S) Codahale Timer with SlidingWindowReservoir configured to hold 100,000 events.
     *
     * Based on results below, implementation (FINAL) was chosen as the final FastTimer implementation, as it
     * achieves nearly the same throughput as (DEV1) at nearly the same memory footprint as (DEV2), and
     * ultimately achieves roughly 3x higher throughput and scalability that Codahale's default implementation
     * at around half the memory footprint.
     *
     * The following results have been collected on an eight core x86 server running at 3.2 GHz (updated
     * timers are shared across 4 threads):
     *
     * Config   Timer Impl             Timers   Threads      ops/ms   Alloc B/op   Kb/TimerPair
     * ----------------------------------------------------------------------------------------
     *  DEV1    FastTimer (Hash 3)          1         4   11487.904        0                253
     *  DEV1    FastTimer (Hash 3)         10         4   22621.702        0                253
     *  DEV1    FastTimer (Hash 3)        100         4   21781.319        0                253
     *  DEV2    FastTimer (Hash 1)          1         4    5138.143        0                 88
     *  DEV2    FastTimer (Hash 1)         10         4   22902.195        0                 88
     *  DEV2    FastTimer (Hash 1)        100         4   19173.085        0                 88
     *  FINAL   FastTimer (Hash 3/1)        1         4    9291.002        0                 99
     *  FINAL   FastTimer (Hash 3/1)       10         4   16379.940        0                 99
     *  FINAL   FastTimer (Hash 3/1)      100         4   16751.020        0                 99
     *  BASE-E  CodahaleTimer               1         4    3845.187       82.609            189
     *  BASE-E  CodahaleTimer              10         4    7262.445       35.035            189
     *  BASE-E  CodahaleTimer             100         4    7051.77        32.843            189
     *  BASE-T  CodahaleTimer/TimeWindow    1         4     102.479       90.851            174
     *  BASE-T  CodahaleTimer/TimeWindow   10         4      68.852       84.812            174
     *  BASE-T  CodahaleTimer/TimeWindow  100         4     153.444      136.436            174
     *  BASE-S  CodahaleTimer/SlidingWdw    1         4    4670.543        0               2103 (size=100000)
     *  BASE-S  CodahaleTimer/SlidingWdw   10         4   13696.168        0               2103
     *  BASE-S  CodahaleTimer/SlidingWdw  100         4   12541.936        0               2103
     *
     * - ops/ms is the number of timer updates per millisecond.
     * - Alloc B/op is the number of bytes allocated per timer update
     * - Kb/TimerPair is the heap footprint per pair of timers (one with fine-grained, one with coarse-grained buckets)
     *
     * The following test results include snapshot creation every 109 timer updates (typically, we would assume
     * snapshot creation to be much less frequent), and show that also with snapshots in the mix, FastTimer outperforms
     * Codahale default Timers both with respect to throughput and scalability as well as object allocation:
     *
     * Config   Timer Impl             Timers   Threads      ops/ms   Alloc B/op
     * -------------------------------------------------------------------------
     *  FINAL   FastTimer (Hash 3/1)        1         4    1569.953       23.707
     *  FINAL   FastTimer (Hash 3/1)       10         4    7316.794       24.073
     *  FINAL   FastTimer (Hash 3/1)      100         4    6498.215       24.073
     *  BASE-E  CodahaleTimer               1         4     246.953      481.771
     *  BASE-E  CodahaleTimer              10         4    1989.134      476.807
     *  BASE-E  CodahaleTimer             100         4    1514.729      468.624
     *  BASE-T  CodahaleTimer/TimeWindow    1         4       6.063    43795.810
     *  BASE-T  CodahaleTimer/TimeWindow   10         4      44.651    33916.315
     *  BASE-T  CodahaleTimer/TimeWindow  100         4     180.431    12330.939
     *  BASE-S  CodahaleTimer/SlidingWdw    1         4      17.439    14683.756
     *  BASE-S  CodahaleTimer/SlidingWdw   10         4     107.257    14683.745
     *  BASE-S  CodahaleTimer/SlidingWdw  100         4     236.538     9767.106
     *
     * Unfortunately Codahale does not have a Timer interface we can implement, and some Codahale
     * base classes are assuming instances of Timer (for example, our JettyServices instantiate a
     * Codahale MetricsServlet, which instantiates a Codahale MetricsModule, which only serializes
     * timers that are instances of Timer class into the json output stream). Unless we wanted to
     * reimplement or override all these base classes, we can't just implement Codahale's Metered and Sampling
     * interfaces. Instead we have to extend its Timer class, even though we're not using any of its
     * inherited functionality or data structures. The inherited (unused) member variables of Codahale Timer
     * consume slightly less than 512 byte per FastTimer (measured around 425 byte in Codahale 3.1).
     * Above memory footprint results include ~ 1 kb of inherited (unused) data structures, which comprise
     * around 1% of FastTimer's overall memory footprint.
     *
     * In terms of functionality, FastTimer provides the same functionality as Codahale's timers
     * (in default configuration with ExponentiallyDecayingReservoirs), with the following exceptions:
     * - Statistics are kept for a fixed amount of time (rather than exponentially decayed), by
     *   default 60 seconds. As a consequence, getMeanRate(), getOneMinuteRate(),  getFiveMinuteRate()
     *   and getFifteenMinuteRate() all return the same value if FastTimer is configured to use a
     *   60 second time window.
     * - FastTimer and FastSnapshot only record bucketized instead of discrete response times. As a
     *   consequence, the accuracy of percentiles depends on bucket granularity. FastSnapshot also
     *   can't return discrete values: getValues() returns an empty array, and size returns 0.
     */

    /**
     * For improved scalability, threads are hased to meters, counters, and min/max values based on
     * HASH_SIZE. Note that response time buckets are *not* hashed to reduce memory footprint, and we
     * assume that concurrent updates of the same response time bucket are infrequent.
     * The hash size could be made configurable in the future (if ever seems necessary). For now, we just
     * hard-code it to 3 based on above performance results.
     */
    private static final int HASH_SIZE = 3;

    /**
     * This timer stores rate and response times on a per-second basis for a configurable amount of time
     * (default: 60 seconds).
     * Note that larger time windows increase the memory footprint of this timer (nearly linear).
     */
    private static final int TIME_WINDOW = 60;

    /*
     * Buckets for percentiles store response times according to the definition in BUCKET_SPEC in the
     * form of { numerOfBuckets , nanosecondResolutionPerBucket }.
     *
     * BUCKET_SPEC_FINE:
     * This bucket definition provides fine-grained timing for small values, and more coarse-grained timing
     * for larger values. We expect this timer to be used primarily for I/O operations that typically
     * range in milliseconds (or sub-milliseconds), with sporadic outliers in the single-digit second
     * range. For values larger than 10 seconds, we only keep the maximum value, but no distribution.
     *
     * BUCKET_SPEC_COARSE:
     * This bucket specification provides coarse-grained timing for events in the range of 1 - 20 seconds
     * with 1 second granularity.
     *
     * If this timer is used for timing of events with significantly different value distribution,
     * other bucket definitions may be specified.
     *
     * Note that a larger number of buckets increases the memory footprint of this timer nearly linear
     * (as the number of buckets largely dominate the timer's overall memory footprint).
     */
    private static final long[][] BUCKET_SPEC_FINE = new long[][] {
        { 100 ,     100000}, // 100 buckets of  0.1 ms (  0.1 -   10.0 ms)
        {  90 ,    1000000}, //  90 buckets of    1 ms (   10 -    100 ms)
        {  90 ,   10000000}, //  90 buckets of   10 ms (  100 -  1,000 ms)
        {   9 , 1000000000}, //   9 buckets of 1000 ms (1,000 - 10,000 ms)
     };                      // + 1 (default) bucket for all values > 10,000 ms

    private static final long[][] BUCKET_SPEC_COARSE = new long[][] {
        {  20 , 1000000000}, //  20 buckets of 1000 ms (1,000 - 20,000 ms)
    };                      // + 1 (default) bucket for all values > 20,000 ms

    /**
     * Defines the response time buckets to use.
     * - fine: BUCKET_SPEC_FINE
     * - coarse: BUCKET_SPEC_COARSE
     * - none: no response time buckets
     */
    public enum Buckets {
        fine,
        coarse,
        none
    }

    // index into the second dimension of BUCKET_SPEC arrays
    private static final int BS_NUMBUCKETS = 0;
    private static final int BS_RESOLUTION = 1;

    /*
     * approximate space requirements for an instance of FastTimer:
     * 4096 + (TIME_WINDOW + 2) * ((HASH_SIZE * 28) + (NUMBUCKETS * 4))
     *
     * For timeWindow=60 and Buckets.fine:   ~ 81 kb
     * For timeWindow=60 and Buckets.coarse: ~ 14 kb
     */

    private final long[][] bucketSpec;
    private final int numBuckets;
    private final long[] bucketBounds;
    private final int timeWindow;
    private final int startTime;

    private final AtomicLong[] counter; // indexed by [hash]
    private final Object[] locks; // indexed by [hash]
    private final int[] lastTime;
    private int lastTimeBucket = 0;

    private final int[][] meter;    // indexed by [hash][time]
    private final int[][] buckets;  // indexed by [bucket][time]
    private final long[][] min;     // indexed by [hash][time]
    private final long[][] max;     // indexed by [hash][time]
    private final long[][] sum;     // indexed by [hash][time]

    /**
     * A Dummy reservoir implementation.
     * Since we have to extend Codahale's Timer class (see above), we inherit all its member
     * objects as well. By default, Timer instantiates a ExponentiallyDecayingReservoir. Since
     * we're not making use of it, we instead instantiate our own DummyReservoir to reduce
     * memory footprint.
     */
    private static class DummyReservoir implements Reservoir {

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void update(long value) {
        }

        @Override
        public Snapshot getSnapshot() {
            return null;
        }

    }

    /**
     * Constructs a new timer with default time window (60 seconds) and
     * default time buckets (fine).
     */
    public FastTimer() {
        this(TIME_WINDOW, Buckets.fine);
    }

    /**
     * Constructs a new timer.
     * @param timeWindowSeconds the time window (in seconds) for this timer
     * @param buckets the type of buckets to use for response times
     */
    public FastTimer(int timeWindowSeconds, Buckets buckets) {
        super(new DummyReservoir());
        this.timeWindow = timeWindowSeconds + 2; // 2 extra seconds for housekeeping

        switch (buckets) {
        case fine:
            bucketSpec = BUCKET_SPEC_FINE;
            break;
        case coarse:
            bucketSpec = BUCKET_SPEC_COARSE;
            break;
        default:
            bucketSpec = null;
        }

        // initialize buckets
        int bucketCnt = 0;
        for (int i = 0; bucketSpec != null && i < bucketSpec.length; i++) {
            bucketCnt += bucketSpec[i][BS_NUMBUCKETS];
        }
        numBuckets = (bucketCnt > 0 ? bucketCnt + 1 : 0);
        if (numBuckets > 0) {
            bucketBounds = new long[bucketSpec.length];
            long bound = 0;
            for (int i = 0; i < bucketSpec.length; i++) {
                bound += bucketSpec[i][BS_NUMBUCKETS] * bucketSpec[i][BS_RESOLUTION];
                bucketBounds[i] = bound;
            }
        } else {
            bucketBounds = null;
        }

        this.startTime = getTime();

        counter = new AtomicLong[HASH_SIZE];
        for (int i = 0; i < counter.length; i++) {
            counter[i] = new AtomicLong(0);
        }
        meter = new int[HASH_SIZE][timeWindow];
        if (numBuckets > 0) {
            this.buckets = new int[numBuckets][timeWindow];
        } else {
            this.buckets = null;
        }
        sum = new long[HASH_SIZE][timeWindow];
        min = new long[HASH_SIZE][timeWindow];
        max = new long[HASH_SIZE][timeWindow];

        lastTime = new int[HASH_SIZE];
        locks = new Object[HASH_SIZE];
        for (int h = 0; h < locks.length; h++) {
            locks[h] = new Object();
        }
    }

    /**
     * Returns the number of response time buckets used by this timer.
     * @return the number of response time buckets
     */
    public int getNumberOfBuckets() {
        return numBuckets;
    }

    /**
     * Figure out which percentile bucket an event of a given duration belongs into.
     * @param duration the duration (in nanoseconds)
     * @return the bucket
     */
    public int getBucket(long duration) {
        if (numBuckets == 0) {
            return -1;
        }
        int bucket = 0;
        long lowbound = 0;
        for (int i = 0; i < bucketSpec.length; i++) {
            if (duration <= bucketBounds[i]) {
                return bucket + (int) ((duration - lowbound - 1) / bucketSpec[i][BS_RESOLUTION]);
            } else {
                bucket += bucketSpec[i][BS_NUMBUCKETS];
                lowbound = bucketBounds[i];
            }
        }
        return numBuckets - 1;
    }

    /**
     * Returns the upper bucket bound (inclusive) of a given bucket.
     * @param b the bucket
     * @return the bound (in nanoseconds)
     */
    public long getBucketBound(int b) {
        if (numBuckets == 0) {
            return -1;
        }
        int bucket = 0;
        long lowbound = 0;
        for (int i = 0; i < bucketSpec.length; i++) {
            if (b < bucket + bucketSpec[i][BS_NUMBUCKETS]) {
                return lowbound + ((long) ((b + 1) - bucket)) * bucketSpec[i][BS_RESOLUTION];
            } else {
                bucket += bucketSpec[i][BS_NUMBUCKETS];
                lowbound = bucketBounds[i];
            }
        }
        return Long.MAX_VALUE;
    }

    /**
     * Returns the average value of a given bucket (the mean between its lower and upper bound).
     * @param b the bucket
     * @return the average value (in nanoseconds)
     */
    public long getBucketValue(int b) {
        if (numBuckets == 0) {
            return -1;
        }
        if (b == 0) {
            return getBucketBound(0) / 2;
        }
        if (b == numBuckets - 1) {
            return 2 * getBucketBound(numBuckets - 2);
        }
        return (getBucketBound(b - 1) + getBucketBound(b)) / 2;
    }

    /**
     * Hashes a thread to a hash index.
     * @return the hash index
     */
    private int getHash() {
        // hashing threads to timers is cheaper than ThreadLocal timers
        return (int) (Thread.currentThread().getId() % HASH_SIZE);
    }

    /**
     * Returns the current absolute time (in seconds).
     * @return the current absolute time (in seconds)
     */
    protected int getTime() {
        return (int) TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    }

    /**
     * Returns the current second (relative to start time) and, if necessary, performs house-keeping.
     * @param hash the hash of the calling thread
     * @return the current time since start (in seconds)
     */
    private int getNow(int hash) {
        int now = getTime() - startTime;

        // check whether we need to do housekeeping
        if (now > lastTime[hash]) {
            synchronized (locks[hash]) {
                // now that we have the lock, check again
                if (now > lastTime[hash]) {
                    int tstop = (now + 2) % timeWindow;

                    // clear meter for next time period
                    for (int t = (lastTime[hash] + 2) % timeWindow; t != tstop; t = (t + 1) % timeWindow) {
                        meter[hash][t] = 0;
                    }

                    // clear histo for next time period
                    for (int t = (lastTime[hash] + 2) % timeWindow; t != tstop; t = (t + 1) % timeWindow) {
                        sum[hash][t] = 0;
                        min[hash][t] = 0;
                        max[hash][t] = 0;
                    }

                    lastTime[hash] = now;
                }
            }
        }

        // check whether we need to do bucket housekeeping
        // (we have to do this separately since buckets aren't hashed)
        if (numBuckets > 0 && now > lastTimeBucket) {
            synchronized (buckets) {
                // now that we have the lock, check again
                if (now > lastTimeBucket) {
                    int tstop = (now + 2) % timeWindow;
                    for (int b = 0; b < numBuckets; b++) {
                        synchronized (buckets[b]) {
                            for (int t = (lastTimeBucket + 2) % timeWindow; t != tstop; t = (t + 1) % timeWindow) {
                                buckets[b][t] = 0;
                            }
                        }
                    }
                    lastTimeBucket = now;
                }
            }
        }

        return now % timeWindow;
    }

    /**
     * Returns the average per-second rate of events this timer has seen.
     * The computed rate is calculated for past seconds (not including the current second, which is still being
     * updated). If the specified time exceeds the time window of this timer, the only rate of the configured time
     * window is reported.
     * @param seconds the number of seconds over which to calculate the average rate
     * @return the average rate (per second).
     */
    public double getRate(int seconds) {
        seconds = Math.min(seconds, timeWindow - 2);
        int t = getNow(getHash()) - 1; // start from last completed second
        int secFrom = t - seconds;
        long sum = 0;
        for (int h = 0; h < HASH_SIZE; h++) {
            for (int i = t; i > secFrom; i--) {
                // no need to synchronize for reading (meter (int) is written atomically)
                sum += meter[h][(timeWindow + i) % timeWindow];
            }
        }
        return ((double) sum) / (double) seconds;
    }

    /**
     * Returns the all-time count of events this timer has seen.
     * @return the all-time count of events
     */
    @Override
    public long getCount() {
        long sum = 0;
        for (AtomicLong c : counter) {
            sum += c.get();
        }
        return sum;
    }

    @Override
    public double getFifteenMinuteRate() {
        return getRate(15 * 60);
    }

    @Override
    public double getFiveMinuteRate() {
        return getRate(5 * 60);
    }

    @Override
    public double getMeanRate() {
        return getRate(Integer.MAX_VALUE);
    }

    @Override
    public double getOneMinuteRate() {
        return getRate(60);
    }

    /**
     * Returns a snapshot of this timer.
     * The computed snapshot is calculated over the complete time interval supported by
     * this timer.
     * @return a snapshot of this timer
     */
    @Override
    public Snapshot getSnapshot() {
        long sum = 0;
        long cnt = 0;
        long min = 0;
        long max = 0;

        // get time and trigger housekeeping
        int now = getNow(0) - 1; // start from last completed second
        int secFrom = now - (timeWindow - 2);

        for (int i = 1; i < HASH_SIZE; i++) {
            getNow(i);
        }

        long[] buckets = (numBuckets > 0 ? new long[numBuckets] : null);
        for (int i = now; i > secFrom; i--) {
            int t = (timeWindow + i) % timeWindow;
            for (int h = 0; h < HASH_SIZE; h++) {
                synchronized (locks[h]) {
                    sum += this.sum[h][t];
                    cnt += this.meter[h][t];
                    if ((this.min[h][t] < min && this.min[h][t] > 0) || min == 0) {
                        min = this.min[h][t];
                    }
                    if (this.max[h][t] > max) {
                        max = this.max[h][t];
                    }
                }
            }
            // no need to synchronize for reading (buckets (int) is written atomically)
            for (int b = 0; b < numBuckets; b++) {
                buckets[b] += this.buckets[b][t];
            }
        }

        return new FastSnapshot(this, min, max, sum, cnt, buckets);
    }

    /**
     * Add an event to this timer.
     * @param duration the time duration of the event
     * @param unit the unit of time duration
     */
    @Override
    public void update(long duration, TimeUnit unit) {
        update(unit.toNanos(duration));
    }

    /**
     * Add an event to this timer.
     * @param duration the time duration of the event (in nanoseconds)
     */
    private void update(long duration) {
        if (duration < 1) {
            // we can't time anything that took less than 1 ns (caller gave us wrong value)
            duration = 1;
        }
        int h = getHash();
        int t = getNow(h);
        counter[h].incrementAndGet();

        int b = getBucket(duration);
        synchronized (locks[h]) {
            meter[h][t]++;
            sum[h][t] += duration;
            if (duration < min[h][t] || min[h][t] == 0) {
                min[h][t] = duration;
            }
            if (duration > max[h][t]) {
                max[h][t] = duration;
            }
        }
        if (numBuckets > 0) {
            synchronized (buckets[b]) {
                buckets[b][t]++;
            }
        }
    }

}
