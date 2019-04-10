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

import com.codahale.metrics.Snapshot;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.OutputStream;

/**
 * A snapshot of a FastTimer.
 */
public class FastSnapshot extends Snapshot {

    private static final long[] EMPTY_VALUES = new long[] {};

    private final FastTimer timer;
    private final long min;
    private final long max;
    private final long sum;
    private final long cnt;
    private final long pcnt;
    private final long[] values;

    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP2",
            justification = "long[] values is newly created array; FastTimer does not hold on to reference")
    public FastSnapshot(FastTimer timer, long min, long max, long sum, long cnt, long[] values) {
        this.timer = timer;
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.cnt = cnt;
        this.pcnt = values != null ? sumOf(values) : 0;
        this.values = values;
    }

    @Override
    public double getValue(double quantile) {
        if (pcnt == 0 || values == null) {
            return 0;
        }
        long qcnt = 0;
        for (int i = 0; i < values.length; i++) {
            qcnt += values[i];
            if (((double) qcnt) / ((double) pcnt) > quantile) {
                return timer.getBucketBound(i);
            }
        }
        return timer.getBucketBound(values.length);
    }

    @Override
    public long[] getValues() {
        return EMPTY_VALUES; // values in this snapshot represent percentile buckets, but not discrete values
    }

    @Override
    public int size() {
        return 0; // values in this snapshot represent percentile buckets, but not discrete values
    }

    @Override
    public long getMax() {
        return max;
    }

    @Override
    public double getMean() {
        return cnt > 0 ? ((double) sum) / ((double) cnt) : 0;
    }

    @Override
    public long getMin() {
        return min;
    }

    @Override
    public double getStdDev() {
        if (cnt < 2 || values == null) {
            return 0;
        }
        double avg = getMean();
        double var = 0;
        for (int i = 0; i < values.length; i++) {
            double val = timer.getBucketValue(i);
            var += ((double) values[i]) * Math.pow(val - avg, 2);
         }
        return Math.sqrt(var / ((double) cnt));
    }

    @Override
    public void dump(OutputStream output) {
        // values in this snapshot represent percentile buckets, but not discrete values
    }

    /**
     * Calculates the sum of values of an array.
     * @param a an array of values
     * @return the sum of all array values
     */
    private long sumOf(long[] a) {
        long sum = 0;
        for (long x : a) {
            sum += x;
        }
        return sum;
    }

}