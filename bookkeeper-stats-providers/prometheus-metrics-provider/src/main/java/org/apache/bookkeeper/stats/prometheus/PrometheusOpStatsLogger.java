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

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Summary;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * A {@code Prometheus} based {@link OpStatsLogger} implementation.
 */
public class PrometheusOpStatsLogger implements OpStatsLogger {

    private final Summary summary;
    private final Summary.Child success;
    private final Summary.Child fail;

    public PrometheusOpStatsLogger(CollectorRegistry registry, String name) {
        this.summary = PrometheusUtil.safeRegister(registry,
                Summary.build().name(name).help("-") //
                        .quantile(0.50, 0.01) //
                        .quantile(0.75, 0.01) //
                        .quantile(0.95, 0.01) //
                        .quantile(0.99, 0.01) //
                        .quantile(0.999, 0.01) //
                        .quantile(0.9999, 0.01) //
                        .quantile(1.0, 0.01) //
                        .maxAgeSeconds(60) //
                        .labelNames("success") //
                        .create());

        this.success = summary.labels("true");
        this.fail = summary.labels("false");
    }

    @Override
    public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
        // Collect latency in millis, truncating anything below micros
        success.observe(unit.toMicros(eventLatency) / 1000.0);
    }

    @Override
    public void registerFailedEvent(long eventLatency, TimeUnit unit) {
        fail.observe(unit.toMicros(eventLatency) / 1000.0);
    }

    @Override
    public void registerSuccessfulValue(long value) {
        success.observe(value);
    }

    @Override
    public void registerFailedValue(long value) {
        fail.observe(value);
    }

    @Override
    public OpStatsData toOpStatsData() {
        // Not relevant as we don't use JMX here
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        summary.clear();
    }

}
