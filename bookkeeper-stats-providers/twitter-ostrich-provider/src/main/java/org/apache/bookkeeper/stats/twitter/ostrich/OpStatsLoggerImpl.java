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
package org.apache.bookkeeper.stats.twitter.ostrich;

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OpStatsLoggerImpl implements OpStatsLogger {

    static final Logger LOG = LoggerFactory.getLogger(OpStatsLoggerImpl.class);

    static final double[] PERCENTILES = new double[] {
            0.1, 0.5, 0.9, 0.99, 0.999, 0.9999
    };

    private final String scope;
    private final com.twitter.ostrich.stats.Counter successCounter;
    private final com.twitter.ostrich.stats.Counter failureCounter;
    private final com.twitter.ostrich.stats.Metric successMetric;
    private final com.twitter.ostrich.stats.Metric failureMetric;

    OpStatsLoggerImpl(String scope, com.twitter.ostrich.stats.StatsProvider statsProvider) {
        this.scope = scope;
        successCounter = statsProvider.getCounter(statName("requests/success"));
        failureCounter = statsProvider.getCounter(statName("requests/failure"));
        successMetric = statsProvider.getMetric(statName("latency/success"));
        failureMetric = statsProvider.getMetric(statName("latency/failure"));
    }

    private String statName(String statName) {
        return String.format("%s/%s", scope, statName);
    }

    @Override
    public void registerFailedEvent(long eventLatency, TimeUnit unit) {
        if (eventLatency < 0) {
            LOG.debug("{} : tried to register negative failure", scope);
        } else {
            failureMetric.add((int) unit.toMillis(eventLatency));
            failureCounter.incr();
        }
    }

    @Override
    public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
        if (eventLatency < 0) {
            LOG.debug("{} : tried to register negative success", scope);
        } else {
            successMetric.add((int) unit.toMillis(eventLatency));
            successCounter.incr();
        }
    }

    @Override
    public void registerSuccessfulValue(long value) {
        if (value < 0) {
            LOG.debug("{} : tried to register negative success", scope);
        } else {
            successMetric.add((int) value);
            successCounter.incr();
        }
    }

    @Override
    public void registerFailedValue(long value) {
        if (value < 0) {
            LOG.debug("{} : tried to register negative success", scope);
        } else {
            failureMetric.add((int) value);
            failureCounter.incr();
        }
    }

    @Override
    public OpStatsData toOpStatsData() {
        long numSuccess = successCounter.apply();
        long numFailures = failureCounter.apply();
        com.twitter.ostrich.stats.Distribution distribution = successMetric.apply();
        com.twitter.ostrich.stats.Histogram histogram = distribution.histogram();
        double avgLatency = distribution.average();
        long[] percentiles = new long[PERCENTILES.length];
        int i = 0;
        for (double percentile : PERCENTILES) {
            percentiles[i] = histogram.getPercentile(percentile);
            ++i;
        }
        return new OpStatsData(numSuccess, numFailures, avgLatency, percentiles);
    }

    @Override
    public void clear() {
        successCounter.reset();
        failureCounter.reset();
        successMetric.clear();
        failureMetric.clear();
    }
}
