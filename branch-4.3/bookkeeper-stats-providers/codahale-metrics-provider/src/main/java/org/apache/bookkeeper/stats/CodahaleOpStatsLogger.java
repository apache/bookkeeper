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
package org.apache.bookkeeper.stats;

import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Snapshot;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class CodahaleOpStatsLogger implements OpStatsLogger {
    final Timer success;
    final Timer fail;

    CodahaleOpStatsLogger(Timer success, Timer fail) {
        this.success = success;
        this.fail = fail;
    }

    // OpStatsLogger functions
    public void registerFailedEvent(long eventLatencyMillis) {
        fail.update(eventLatencyMillis, TimeUnit.MILLISECONDS);
    }

    public void registerSuccessfulEvent(long eventLatencyMillis) {
        success.update(eventLatencyMillis, TimeUnit.MILLISECONDS);
    }

    public synchronized void clear() {
        // can't clear a timer
    }

    /**
     * This function should go away soon (hopefully).
     */
    public synchronized OpStatsData toOpStatsData() {
        long numFailed = fail.getCount();
        long numSuccess = success.getCount();
        Snapshot s = success.getSnapshot();
        double avgLatencyMillis = s.getMean();

        double[] default_percentiles = {10, 50, 90, 99, 99.9, 99.99};
        long[] latenciesMillis = new long[default_percentiles.length];
        Arrays.fill(latenciesMillis, Long.MAX_VALUE);
        for (int i = 0; i < default_percentiles.length; i++) {
            latenciesMillis[i] = (long)s.getValue(default_percentiles[i]);
        }
        return new OpStatsData(numSuccess, numFailed, avgLatencyMillis, latenciesMillis);
    }
}
