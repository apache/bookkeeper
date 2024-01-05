/*
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
package org.apache.bookkeeper.stats.otel;

import io.opentelemetry.api.common.Attributes;
import java.util.StringJoiner;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

public class OtelStatsLogger implements StatsLogger {

    private final OtelMetricsProvider provider;
    private final String scope;
    private final Attributes attributes;

    OtelStatsLogger(OtelMetricsProvider provider, String scope, Attributes attributes) {
        this.provider = provider;
        this.scope = scope;
        this.attributes = attributes;
    }

    @Override
    public OpStatsLogger getOpStatsLogger(String name) {
        return provider.opStats.computeIfAbsent(scopeContext(name), sc -> new OtelOpStatsLogger(provider.meter, sc));
    }

    @Override
    public OpStatsLogger getThreadScopedOpStatsLogger(String name) {
        return getOpStatsLogger(name);
    }

    @Override
    public Counter getCounter(String name) {
        return provider.counters.computeIfAbsent(scopeContext(name), sc -> new OtelCounter(provider.meter, sc));
    }

    @Override
    public Counter getThreadScopedCounter(String name) {
        return getCounter(name);
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
        provider.meter.gaugeBuilder(completeName(name))
                .buildWithCallback(observableDoubleMeasurement -> {
                    double v = gauge.getSample().doubleValue();
                    observableDoubleMeasurement.record(v, attributes);
                });
    }

    @Override
    public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
        // no-op
    }

    @Override
    public StatsLogger scope(String name) {
        return new OtelStatsLogger(provider, completeName(name), attributes);
    }

    @Override
    public void removeScope(String name, StatsLogger statsLogger) {
        // no-op
    }

    @Override
    public StatsLogger scopeLabel(String labelName, String labelValue) {
        Attributes newAttributes = Attributes.builder()
                .putAll(attributes)
                .put(labelName, labelValue)
                .build();
        return new OtelStatsLogger(provider, scope, newAttributes);
    }

    private ScopeContext scopeContext(String name) {
        return new ScopeContext(completeName(name), attributes);
    }

    private String completeName(String name) {
        String metricName = scope.isEmpty()
                ? name
                : new StringJoiner(".").add(scope).add(name).toString();

        return metricName;
    }
}
