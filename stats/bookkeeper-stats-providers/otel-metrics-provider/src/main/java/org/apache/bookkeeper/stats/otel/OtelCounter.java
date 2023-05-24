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
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.Counter;

 class OtelCounter implements Counter {
    private final LongUpDownCounter counter;
    private final Attributes attributes;

    OtelCounter(Meter meter, ScopeContext sc) {
        this.counter = meter.upDownCounterBuilder(sc.getName()).build();
        this.attributes = sc.getAttributes();
    }

    @Override
    public void clear() {
        // no-op
    }

    @Override
    public void inc() {
        counter.add(1, attributes);
    }

    @Override
    public void dec() {
        counter.add(-1, attributes);
    }

    @Override
    public void addCount(long delta) {
        counter.add(delta, attributes);
    }

    @Override
    public void addLatency(long eventLatency, TimeUnit unit) {
        long valueMillis = unit.toMillis(eventLatency);
        counter.add(valueMillis);
    }

    @Override
    public Long get() {
        return -1L;
    }
}
