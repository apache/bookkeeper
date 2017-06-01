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
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;

/**
 * Main entry point to use Finagle stats for Bookkeeper.
 *
 * There's no requirement to start or stop it.
 */
public class FinagleStatsProvider implements StatsProvider {
    final private StatsReceiver stats;

    public FinagleStatsProvider(final StatsReceiver stats) {
        this.stats = stats;
    }

    @Override
    public void start(Configuration conf) { /* no-op */ }

    @Override
    public void stop() { /* no-op */ }

    @Override
    public StatsLogger getStatsLogger(final String scope) {
        return new FinagleStatsLoggerImpl(this.stats.scope(scope));
    }
}
