package org.apache.bookkeeper.stats.codahale;

import com.codahale.metrics.Snapshot;
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
    private final long[] values;

    public FastSnapshot(FastTimer timer, long min, long max, long sum, long cnt, long[] values) {
        this.timer = timer;
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.cnt = cnt;
        this.values = values;
    }

    @Override
    public double getValue(double quantile) {
        if (cnt == 0 || values == null) {
            return 0;
        }
        long qcnt = 0;
        for (int i = 0; i < values.length; i++) {
            qcnt += values[i];
            if (((double) qcnt) / ((double) cnt) > quantile) {
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

}