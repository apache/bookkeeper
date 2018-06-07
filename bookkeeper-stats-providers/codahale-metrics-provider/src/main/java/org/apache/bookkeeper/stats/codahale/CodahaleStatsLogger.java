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
package org.apache.bookkeeper.stats.codahale;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@link StatsLogger} implemented based on <i>Codahale</i> metrics library.
 */
public class CodahaleStatsLogger implements StatsLogger {
    protected final String basename;
    final MetricRegistry metrics;

    CodahaleStatsLogger(MetricRegistry metrics, String basename) {
        this.metrics = metrics;
        this.basename = basename;
    }

    @Override
    public OpStatsLogger getOpStatsLogger(String statName) {
        Timer success = metrics.timer(name(basename, statName));
        Timer failure = metrics.timer(name(basename, statName + "-fail"));
        return new CodahaleOpStatsLogger(success, failure);
    }

    @Override
    public Counter getCounter(String statName) {
        final com.codahale.metrics.Counter c = metrics.counter(name(basename, statName));
        return new Counter() {
            @Override
            public synchronized void clear() {
                long cur = c.getCount();
                c.dec(cur);
            }

            @Override
            public Long get() {
                return c.getCount();
            }

            @Override
            public void inc() {
                c.inc();
            }

            @Override
            public void dec() {
                c.dec();
            }

            @Override
            public void add(long delta) {
                c.inc(delta);
            }
        };
    }

    @Override
    public <T extends Number> void registerGauge(final String statName, final Gauge<T> gauge) {
        String metricName = name(basename, statName);
        metrics.remove(metricName);

        metrics.register(metricName, new com.codahale.metrics.Gauge<T>() {
                @Override
                public T getValue() {
                    return gauge.getSample();
                }
            });
    }

    @Override
    public <T extends Number> void unregisterGauge(String statName, Gauge<T> gauge) {
        // do nothing right now as the Codahale doesn't support conditional removal
    }

    @Override
    public StatsLogger scope(String scope) {
        String scopeName;
        if (basename == null || 0 == basename.length()) {
            scopeName = scope;
        } else {
            scopeName = name(basename, scope);
        }
        return new CodahaleStatsLogger(metrics, scopeName);
    }

    @Override
    public void removeScope(String name, StatsLogger statsLogger) {
        // no-op. the codahale stats logger doesn't have the references for stats logger.
    }
}
