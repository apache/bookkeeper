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

import com.twitter.common.stats.SampledStat;
import com.twitter.common.stats.Stats;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Implementation of twitter-stats logger.
 */
public class TwitterStatsLoggerImpl implements StatsLogger {

    protected final String name;

    public TwitterStatsLoggerImpl(String name) {
        this.name = name;
    }

    @Override
    public OpStatsLogger getOpStatsLogger(String statName) {
        return new OpStatsLoggerImpl(getStatName(statName));
    }

    @Override
    public Counter getCounter(String statName) {
        return new CounterImpl(getStatName(statName));
    }

    @Override
    public <T extends Number> void registerGauge(final String statName, final Gauge<T> gauge) {
        Stats.export(new SampledStat<Number>(getStatName(statName), gauge.getDefaultValue()) {
            @Override
            public T doSample() {
                return gauge.getSample();
            }
        });
    }

    @Override
    public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
        // no-op
    }

    private String getStatName(String statName) {
        return (name + "_" + statName).toLowerCase();
    }

    @Override
    public StatsLogger scope(String scope) {
        String scopeName;
        if (0 == name.length()) {
            scopeName = scope;
        } else {
            scopeName = name + "_" + scope;
        }
        return new TwitterStatsLoggerImpl(scopeName);
    }

    @Override
    public void removeScope(String name, StatsLogger statsLogger) {
        // no-op
    }
}
