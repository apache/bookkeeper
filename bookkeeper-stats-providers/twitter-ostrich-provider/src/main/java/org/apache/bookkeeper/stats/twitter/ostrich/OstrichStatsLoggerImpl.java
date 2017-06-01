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

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import scala.Function0;
import scala.runtime.AbstractFunction0;

/**
 * Implementation of ostrich logger.
 */
class OstrichStatsLoggerImpl implements StatsLogger {

    protected final String scope;
    protected final com.twitter.ostrich.stats.StatsProvider ostrichProvider;

    OstrichStatsLoggerImpl(String scope, com.twitter.ostrich.stats.StatsProvider ostrichProvider) {
        this.scope = scope;
        this.ostrichProvider = ostrichProvider;
    }

    @Override
    public OpStatsLogger getOpStatsLogger(String statName) {
        return new OpStatsLoggerImpl(getStatName(statName), ostrichProvider);
    }

    @Override
    public Counter getCounter(String statName) {
        return new CounterImpl(ostrichProvider.getCounter(getStatName(statName)));
    }

    @Override
    public <T extends Number> void registerGauge(final String statName, final Gauge<T> gauge) {
        Function0<Object> gaugeFunc = new AbstractFunction0<Object>() {
            @Override
            public Object apply() {
                return gauge.getSample().doubleValue();
            }
        };
        ostrichProvider.addGauge(getStatName(statName), gaugeFunc);
    }

    @Override
    public <T extends Number> void unregisterGauge(String statName, Gauge<T> gauge) {
        ostrichProvider.clearGauge(getStatName(statName));
    }

    private String getStatName(String statName) {
        return String.format("%s/%s", scope, statName);
    }

    @Override
    public StatsLogger scope(String scope) {
        return new OstrichStatsLoggerImpl(getStatName(scope), ostrichProvider);
    }

    @Override
    public void removeScope(String name, StatsLogger statsLogger) {
        // no-op
    }
}
