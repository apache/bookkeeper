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

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import org.apache.bookkeeper.stats.Counter;

/**
 * A {@link Counter} implementation based on <i>Prometheus</i> metrics library.
 */
public class PrometheusCounter implements Counter {

    private final Gauge gauge;

    public PrometheusCounter(CollectorRegistry registry, String name) {
        this.gauge = PrometheusUtil.safeRegister(registry,
                Gauge.build().name(Collector.sanitizeMetricName(name)).help("-").create());
    }

    @Override
    public void clear() {
        gauge.clear();
    }

    @Override
    public void inc() {
        gauge.inc();
    }

    @Override
    public void dec() {
        gauge.dec();
    }

    @Override
    public void add(long delta) {
        gauge.inc(delta);
    }

    @Override
    public Long get() {
        return (long) gauge.get();
    }

}
