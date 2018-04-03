/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.common.rate;

import com.google.common.base.Ticker;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Sampled {@link MovingAverageRate}.
 */
class SampledMovingAverageRate implements MovingAverageRate {

    private static final long NANOS_PER_SEC = TimeUnit.SECONDS.toNanos(1);

    private final LongAdder total;
    private final Ticker ticker;
    private final double scaleFactor;
    private final LinkedBlockingDeque<Pair<Long, Long>> samples;

    private double value;

    public SampledMovingAverageRate(int intervalSecs) {
        this(intervalSecs, 1, Ticker.systemTicker());
    }

    SampledMovingAverageRate(int intervalSecs,
                             double scaleFactor,
                             Ticker ticker) {
        this.value = 0;
        this.total = new LongAdder();
        this.scaleFactor = scaleFactor;
        this.ticker = ticker;
        this.samples = new LinkedBlockingDeque<>(intervalSecs);
    }

    @Override
    public double get() {
        return value;
    }

    @Override
    public void add(long amount) {
        total.add(amount);
    }

    @Override
    public void inc() {
        add(1);
    }

    void sample() {
        value = doSample();
    }

    private double doSample() {
        long newSample = total.sum();
        long newTimestamp = ticker.read();

        double rate = 0;
        if (!samples.isEmpty()) {
            Pair<Long, Long> oldestSample = samples.peekLast();

            double dy = newSample - oldestSample.getRight();
            double dt = newTimestamp - oldestSample.getLeft();

            rate = (dt == 0) ? 0 : (NANOS_PER_SEC * scaleFactor * dy) / dt;
        }

        if (samples.remainingCapacity() == 0) {
            samples.removeLast();
        } else {
            samples.addFirst(Pair.of(newTimestamp, newSample));
        }

        return rate;
    }
}
