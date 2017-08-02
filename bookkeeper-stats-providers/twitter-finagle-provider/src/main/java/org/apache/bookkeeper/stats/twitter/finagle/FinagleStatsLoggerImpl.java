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
import com.twitter.util.Function0;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import scala.collection.Seq;

/**
 * A <i>Finagle Stats</i> library based {@link StatsLogger} implementation.
 */
public class FinagleStatsLoggerImpl implements StatsLogger {
    private final StatsReceiver stats;
    // keep the references for finagle gauges. they are destroyed when the stats logger is destroyed.
    final Map<Gauge, com.twitter.finagle.stats.Gauge> finagleGauges;

    public FinagleStatsLoggerImpl(final StatsReceiver stats) {
        this.stats = stats;
        this.finagleGauges = new HashMap<Gauge, com.twitter.finagle.stats.Gauge>();
    }

    @Override
    public OpStatsLogger getOpStatsLogger(final String name) {
        return new OpStatsLoggerImpl(name, this.stats);
    }

    @Override
    public Counter getCounter(final String name) {
        return new CounterImpl(name, this.stats);
    }

    @Override
    public <T extends Number> void registerGauge(final String name, final Gauge<T> gauge) {
        // This is done to inter-op with Scala Seq
        final Seq<String> gaugeName = scala.collection.JavaConversions.asScalaBuffer(Arrays.asList(name)).toList();
        synchronized (finagleGauges) {
            finagleGauges.put(gauge, this.stats.addGauge(gaugeName, gaugeProvider(gauge)));
        }
    }

    @Override
    public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
        synchronized (finagleGauges) {
            finagleGauges.remove(gauge);
        }
    }

    private <T extends Number> Function0<Object> gaugeProvider(final Gauge<T> gauge) {
        return new Function0<Object>() {
            @Override
            public Object apply() {
                return gauge.getSample().floatValue();
            }
        };
    }

    @Override
    public StatsLogger scope(String name) {
        return new FinagleStatsLoggerImpl(this.stats.scope(name));
    }

    @Override
    public void removeScope(String name, StatsLogger statsLogger) {
        // no-op
    }
}
