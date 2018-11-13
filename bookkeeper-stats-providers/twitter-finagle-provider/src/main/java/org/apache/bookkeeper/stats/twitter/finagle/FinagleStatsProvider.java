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

import com.twitter.finagle.stats.StatsReceiver;
import org.apache.bookkeeper.stats.CachingStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;

/**
 * Main entry point to use Finagle stats for Bookkeeper.
 *
 * <p>There's no requirement to start or stop it.</p>
 */
public class FinagleStatsProvider implements StatsProvider {
    private final StatsReceiver stats;
    private final CachingStatsProvider cachingStatsProvider;

    public FinagleStatsProvider(final StatsReceiver stats) {
        this.stats = stats;
        this.cachingStatsProvider = new CachingStatsProvider(
            new StatsProvider() {
                @Override
                public void start(Configuration conf) {
                    // nop
                }

                @Override
                public void stop() {
                    // nop
                }

                @Override
                public StatsLogger getStatsLogger(String scope) {
                    return new FinagleStatsLoggerImpl(stats.scope(scope));
                }
            }
        );
    }

    @Override
    public void start(Configuration conf) { /* no-op */ }

    @Override
    public void stop() { /* no-op */ }

    @Override
    public StatsLogger getStatsLogger(final String scope) {
        return this.cachingStatsProvider.getStatsLogger(scope);
    }

    @Override
    public String getStatsName(String... statsComponents) {
        return cachingStatsProvider.getStatsName(statsComponents);
    }
}
