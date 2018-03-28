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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@code StatsLogger} that caches the stats objects created by other {@code StatsLogger}.
 */
public class CachingStatsLogger implements StatsLogger {

    protected final StatsLogger underlying;
    protected final ConcurrentMap<String, Counter> counters;
    protected final ConcurrentMap<String, OpStatsLogger> opStatsLoggers;
    protected final ConcurrentMap<String, StatsLogger> scopeStatsLoggers;

    public CachingStatsLogger(StatsLogger statsLogger) {
        this.underlying = statsLogger;
        this.counters = new ConcurrentHashMap<String, Counter>();
        this.opStatsLoggers = new ConcurrentHashMap<String, OpStatsLogger>();
        this.scopeStatsLoggers = new ConcurrentHashMap<String, StatsLogger>();
    }

    @Override
    public int hashCode() {
        return underlying.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CachingStatsLogger)) {
            return false;
        }
        CachingStatsLogger another = (CachingStatsLogger) obj;
        return underlying.equals(another.underlying);
    }

    @Override
    public String toString() {
        return underlying.toString();
    }

    @Override
    public OpStatsLogger getOpStatsLogger(String name) {
        OpStatsLogger opStatsLogger = opStatsLoggers.get(name);
        if (null == opStatsLogger) {
            OpStatsLogger newOpStatsLogger = underlying.getOpStatsLogger(name);
            OpStatsLogger oldOpStatsLogger = opStatsLoggers.putIfAbsent(name, newOpStatsLogger);
            opStatsLogger = (null == oldOpStatsLogger) ? newOpStatsLogger : oldOpStatsLogger;
        }
        return opStatsLogger;
    }

    @Override
    public Counter getCounter(String name) {
        Counter counter = counters.get(name);
        if (null == counter) {
            Counter newCounter = underlying.getCounter(name);
            Counter oldCounter = counters.putIfAbsent(name, newCounter);
            counter = (null == oldCounter) ? newCounter : oldCounter;
        }
        return counter;
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
        underlying.registerGauge(name, gauge);
    }

    @Override
    public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
        underlying.unregisterGauge(name, gauge);
    }

    @Override
    public StatsLogger scope(String name) {
        StatsLogger statsLogger = scopeStatsLoggers.get(name);
        if (null == statsLogger) {
            StatsLogger newStatsLogger = new CachingStatsLogger(underlying.scope(name));
            StatsLogger oldStatsLogger = scopeStatsLoggers.putIfAbsent(name, newStatsLogger);
            statsLogger = (null == oldStatsLogger) ? newStatsLogger : oldStatsLogger;
        }
        return statsLogger;
    }

    @Override
    public void removeScope(String name, StatsLogger statsLogger) {
        scopeStatsLoggers.remove(name, statsLogger);
    }
}
