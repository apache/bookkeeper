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
package org.apache.bookkeeper.stats.prometheus;

import com.google.common.base.Joiner;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@code Prometheus} based {@link StatsLogger} implementation.
 */
public class PrometheusStatsLogger implements StatsLogger {

    private final CollectorRegistry registry;
    private final String scope;

    PrometheusStatsLogger(CollectorRegistry registry, String scope) {
        this.registry = registry;
        this.scope = scope;
    }

    @Override
    public OpStatsLogger getOpStatsLogger(String name) {
        return new PrometheusOpStatsLogger(registry, completeName(name));
    }

    @Override
    public Counter getCounter(String name) {
        return new PrometheusCounter(registry, completeName(name));
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
        PrometheusUtil.safeRegister(registry, io.prometheus.client.Gauge.build().name(completeName(name)).help("-")
                .create().setChild(new io.prometheus.client.Gauge.Child() {
                    @Override
                    public double get() {
                        Number value = null;
                        try {
                            value = gauge.getSample();
                        } catch (Exception e) {
                            // no-op
                        }

                        if (value == null) {
                            value = gauge.getDefaultValue();
                        }
                        return value.doubleValue();
                    }
                }));
    }

    @Override
    public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
        // no-op
    }

    @Override
    public void removeScope(String name, StatsLogger statsLogger) {
        // no-op
    }

    @Override
    public StatsLogger scope(String name) {
        return new PrometheusStatsLogger(registry, completeName(name));
    }

    private String completeName(String name) {
        String completeName = scope.isEmpty() ? name : Joiner.on('_').join(scope, name);
        return Collector.sanitizeMetricName(completeName);
    }
}
