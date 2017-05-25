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
package org.apache.bookkeeper.stats.twitter.finagle;

import com.twitter.finagle.stats.Stat;
import com.twitter.finagle.stats.StatsReceiver;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * A percentile stat that will delegate to Finagle stats' implementation library
 * to compute the percentiles.
 *
 * Note: metrics will be exposed in form $name/success.p99 for successful events,
 * and $name/failure.p99 for failed ones.
 */
public class OpStatsLoggerImpl implements OpStatsLogger {
    final private static OpStatsData NULL_OP_STATS = new OpStatsData(0, 0, 0, new long[6]);
    final private Stat success;
    final private Stat failure;

    public OpStatsLoggerImpl(final String name, final StatsReceiver stats) {
        this.success = stats.scope(String.format("%s/success", name)).stat0(name);
        this.failure = stats.scope(String.format("%s/failure", name)).stat0(name);
    }

    @Override
    public void registerSuccessfulEvent(final long eventLatencyMicros) {
        this.success.add(eventLatencyMicros);
    }

    @Override
    public void registerFailedEvent(final long eventLatencyMicros) {
        this.failure.add(eventLatencyMicros);
    }

    /**
     * We don't need to support percentiles as a part of this provider,
     * since they're part of the Stats implementation library.
     *
     * @return dummy null-stats object
     */
    @Override
    public OpStatsData toOpStatsData() {
        return NULL_OP_STATS;
    }

    @Override
    public void clear() { /* not supported */ }
}
