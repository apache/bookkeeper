/**
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

package org.apache.bookkeeper.proto;

import java.beans.ConstructorProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bookie Server Stats.
 */
public class BKStats {
    private static final Logger LOG = LoggerFactory.getLogger(BKStats.class);
    private static BKStats instance = new BKStats();

    public static BKStats getInstance() {
        return instance;
    }

    /**
     * A read view of stats, also used in CompositeViewData to expose to JMX.
     */
    public static class OpStatData {
        private final long maxLatency, minLatency;
        private final double avgLatency;
        private final long numSuccessOps, numFailedOps;
        private final String latencyHist;

        @ConstructorProperties({"maxLatency", "minLatency", "avgLatency",
                                "numSuccessOps", "numFailedOps", "latencyHist"})
        public OpStatData(long maxLatency, long minLatency, double avgLatency,
                          long numSuccessOps, long numFailedOps, String latencyHist) {
            this.maxLatency = maxLatency;
            this.minLatency = minLatency == Long.MAX_VALUE ? 0 : minLatency;
            this.avgLatency = avgLatency;
            this.numSuccessOps = numSuccessOps;
            this.numFailedOps = numFailedOps;
            this.latencyHist = latencyHist;
        }

        public long getMaxLatency() {
            return maxLatency;
        }

        public long getMinLatency() {
            return minLatency;
        }

        public double getAvgLatency() {
            return avgLatency;
        }

        public long getNumSuccessOps() {
            return numSuccessOps;
        }

        public long getNumFailedOps() {
            return numFailedOps;
        }

        public String getLatencyHist() {
            return latencyHist;
        }
    }

    /**
     * Operation Statistics.
     */
    public static class OpStats {
        static final int NUM_BUCKETS = 3 * 9 + 2;

        long maxLatency = 0;
        long minLatency = Long.MAX_VALUE;
        double totalLatency = 0.0f;
        long numSuccessOps = 0;
        long numFailedOps = 0;
        long[] latencyBuckets = new long[NUM_BUCKETS];

        OpStats() {}

        /**
         * Increment number of failed operations.
         */
        public synchronized void incrementFailedOps() {
            ++numFailedOps;
        }

        /**
         * Update Latency.
         */
        public synchronized void updateLatency(long latency) {
            if (latency < 0) {
                // less than 0ms . Ideally this should not happen.
                // We have seen this latency negative in some cases due to the
                // behaviors of JVM. Ignoring the statistics updation for such
                // cases.
                LOG.warn("Latency time coming negative");
                return;
            }
            totalLatency += latency;
            ++numSuccessOps;
            if (latency < minLatency) {
                minLatency = latency;
            }
            if (latency > maxLatency) {
                maxLatency = latency;
            }
            int bucket;
            if (latency <= 100) { // less than 100ms
                bucket = (int) (latency / 10);
            } else if (latency <= 1000) { // 100ms ~ 1000ms
                bucket = 1 * 9 + (int) (latency / 100);
            } else if (latency <= 10000) { // 1s ~ 10s
                bucket = 2 * 9 + (int) (latency / 1000);
            } else { // more than 10s
                bucket = 3 * 9 + 1;
            }
            ++latencyBuckets[bucket];
        }

        public OpStatData toOpStatData() {
            double avgLatency = numSuccessOps > 0 ? totalLatency / numSuccessOps : 0.0f;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < NUM_BUCKETS; i++) {
                sb.append(latencyBuckets[i]);
                if (i != NUM_BUCKETS - 1) {
                    sb.append(',');
                }
            }

            return new OpStatData(maxLatency, minLatency, avgLatency, numSuccessOps, numFailedOps, sb.toString());
        }

        /**
         * Diff with base opstats.
         *
         * @param base
         *        base opstats
         * @return diff opstats
         */
        public OpStats diff(OpStats base) {
            OpStats diff = new OpStats();
            diff.maxLatency = this.maxLatency > base.maxLatency ? this.maxLatency : base.maxLatency;
            diff.minLatency = this.minLatency > base.minLatency ? base.minLatency : this.minLatency;
            diff.totalLatency = this.totalLatency - base.totalLatency;
            diff.numSuccessOps = this.numSuccessOps - base.numSuccessOps;
            diff.numFailedOps = this.numFailedOps - base.numFailedOps;
            for (int i = 0; i < NUM_BUCKETS; i++) {
                diff.latencyBuckets[i] = this.latencyBuckets[i] - base.latencyBuckets[i];
            }
            return diff;
        }

        /**
         * Copy stats from other OpStats.
         *
         * @param other other op stats
         */
        public synchronized void copyOf(OpStats other) {
            this.maxLatency = other.maxLatency;
            this.minLatency = other.minLatency;
            this.totalLatency = other.totalLatency;
            this.numSuccessOps = other.numSuccessOps;
            this.numFailedOps = other.numFailedOps;
            System.arraycopy(other.latencyBuckets, 0, this.latencyBuckets, 0, this.latencyBuckets.length);
        }
    }

    public static final int STATS_ADD = 0;
    public static final int STATS_READ = 1;
    public static final int STATS_UNKNOWN = 2;
    // NOTE: if add other stats, increment NUM_STATS
    public static final int NUM_STATS = 3;

    OpStats[] stats = new OpStats[NUM_STATS];

    private BKStats() {
        for (int i = 0; i < NUM_STATS; i++) {
            stats[i] = new OpStats();
        }
    }

    /**
     * Stats of operations.
     *
     * @return op stats
     */
    public OpStats getOpStats(int type) {
        return stats[type];
    }

    /**
     * Set stats of a specified operation.
     *
     * @param type operation type
     * @param stat operation stats
     */
    public void setOpStats(int type, OpStats stat) {
        stats[type] = stat;
    }

    /**
     * Diff with base stats.
     *
     * @param base base stats
     * @return diff stats
     */
    public BKStats diff(BKStats base) {
        BKStats diff = new BKStats();
        for (int i = 0; i < NUM_STATS; i++) {
            diff.setOpStats(i, stats[i].diff(base.getOpStats(i)));
        }
        return diff;
    }

    /**
     * Copy stats from other stats.
     *
     * @param other other stats
     */
    public void copyOf(BKStats other) {
        for (int i = 0; i < NUM_STATS; i++) {
            stats[i].copyOf(other.getOpStats(i));
        }
    }
}
