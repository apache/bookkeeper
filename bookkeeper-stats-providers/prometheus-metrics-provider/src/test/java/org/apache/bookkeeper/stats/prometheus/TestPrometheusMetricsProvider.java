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

import static org.junit.Assert.assertEquals;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import java.lang.reflect.Field;
import java.util.Map;
import org.junit.Test;

public class TestPrometheusMetricsProvider {

    private final CollectorRegistry registry = new CollectorRegistry();

    @Test
    public void testCounter() {
        PrometheusCounter counter = new PrometheusCounter(registry, "testcounter");
        long value = counter.get();
        assertEquals(0L, value);
        counter.inc();
        assertEquals(1L, counter.get().longValue());
        counter.dec();
        assertEquals(0L, counter.get().longValue());
        counter.add(3);
        assertEquals(3L, counter.get().longValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTwoCounters() throws Exception {
        PrometheusCounter counter1 = new PrometheusCounter(registry, "testcounter");
        PrometheusCounter counter2 = new PrometheusCounter(registry, "testcounter");
        Field collectorsMapField = CollectorRegistry.class.getDeclaredField("namesToCollectors");
        collectorsMapField.setAccessible(true);
        Map<String, Collector> collectorMap = (Map<String, Collector>) collectorsMapField.get(registry);
        assertEquals(1, collectorMap.size());
    }

}
