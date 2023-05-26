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
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;

class OtelOpStatsLogger implements OpStatsLogger {

    private final DoubleHistogram histo;
    private final Attributes successAttributes;
    private final Attributes failureAttributes;

    OtelOpStatsLogger(Meter meter, ScopeContext sc) {
        this.histo = meter.histogramBuilder(sc.getName()).build();
        this.successAttributes = Attributes.builder().putAll(sc.getAttributes()).put("success", "true").build();
        this.failureAttributes = Attributes.builder().putAll(sc.getAttributes()).put("success", "false").build();
    }

    @Override
    public void registerFailedEvent(long eventLatency, TimeUnit unit) {
        double valueMillis = unit.toMicros(eventLatency) / 1000.0;
        histo.record(valueMillis, failureAttributes);
    }

    @Override
    public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
        double valueMillis = unit.toMicros(eventLatency) / 1000.0;
        histo.record(valueMillis, successAttributes);
    }

    @Override
    public void registerSuccessfulValue(long value) {
        histo.record(value, successAttributes);
    }

    @Override
    public void registerFailedValue(long value) {
        histo.record(value, failureAttributes);
    }

    @Override
    public OpStatsData toOpStatsData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
