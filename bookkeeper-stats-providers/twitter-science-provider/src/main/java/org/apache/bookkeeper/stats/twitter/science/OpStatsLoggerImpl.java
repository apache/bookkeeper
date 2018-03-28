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
package org.apache.bookkeeper.stats.twitter.science;

import com.twitter.common.stats.RequestStats;
import com.twitter.common.stats.Stat;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * An implementation of the OpStatsLogger interface that handles per operation type stats.
 * Internals use twitter.common.stats for exporting metrics.
 */
public class OpStatsLoggerImpl implements OpStatsLogger {
    private final RequestStats events;

    public OpStatsLoggerImpl(String name) {
        this.events = new RequestStats(name);
    }

    // OpStatsLogger functions
    public void registerFailedEvent(long eventLatency, TimeUnit unit) {
        this.events.incErrors(unit.toMicros(eventLatency));
    }

    public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
        this.events.requestComplete(unit.toMicros(eventLatency));
    }

    public void registerSuccessfulValue(long value) {
        this.events.requestComplete(TimeUnit.MILLISECONDS.toMicros(value));
    }

    public void registerFailedValue(long value) {
        this.events.incErrors(TimeUnit.MILLISECONDS.toMicros(value));
    }

    public synchronized void clear() {
    }

    /**
     * This function should go away soon (hopefully).
     */
    public synchronized OpStatsData toOpStatsData() {
        long numFailed = this.events.getErrorCount();
        long numSuccess = this.events.getSlidingStats().getEventCounter().get() - numFailed;
        double avgLatencyMillis = this.events.getSlidingStats().getPerEventLatency().read() / 1000.0;
        double[] defaultPercentiles = {10, 50, 90, 99, 99.9, 99.99};
        long[] latenciesMillis = new long[defaultPercentiles.length];
        Arrays.fill(latenciesMillis, Long.MAX_VALUE);
        Map<Double, ? extends Stat> realPercentileLatencies =
                this.events.getPercentile().getPercentiles();
        for (int i = 0; i < defaultPercentiles.length; i++) {
            if (realPercentileLatencies.containsKey(defaultPercentiles[i])) {
                @SuppressWarnings("unchecked")
                Stat<Double> latency = realPercentileLatencies.get(defaultPercentiles[i]);
                latenciesMillis[i] = TimeUnit.MICROSECONDS.toMillis(latency.read().longValue());
            }
        }
        return new OpStatsData(numSuccess, numFailed, avgLatencyMillis, latenciesMillis);
    }
}
