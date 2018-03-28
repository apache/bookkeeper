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

import com.twitter.common.stats.Rate;
import com.twitter.common.stats.Stats;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.stats.Counter;

/**
 * This will export the value and the rate (per sec) to {@link org.apache.bookkeeper.stats.Stats}.
 */
public class CounterImpl implements Counter {
    // The name used to export this stat
    private String name;
    private AtomicLong value;

    public CounterImpl(String name) {
        this.name = name;
        value = new AtomicLong(0);
        setUpStatsExport();
    }

    @Override
    public synchronized void clear() {
        value.getAndSet(0);
    }

    @Override
    public Long get() {
        return value.get();
    }

    private void setUpStatsExport() {
        // Export the value.
        Stats.export(name, value);
        // Export the rate of this value.
        Stats.export(Rate.of(name + "_per_sec", value).build());
    }

    @Override
    public void inc() {
        value.incrementAndGet();
    }

    @Override
    public void dec() {
        value.decrementAndGet();
    }

    @Override
    public void add(long delta) {
        value.addAndGet(delta);
    }
}
