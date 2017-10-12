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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Container for Prometheus utility methods.
 *
 */
public class PrometheusUtil {

    private static final Field collectorsMapField;
    private static final Method collectorsNamesMethod;

    static {
        try {
            collectorsMapField = CollectorRegistry.class.getDeclaredField("namesToCollectors");
            collectorsMapField.setAccessible(true);

            collectorsNamesMethod = CollectorRegistry.class.getDeclaredMethod("collectorNames", Collector.class);
            collectorsNamesMethod.setAccessible(true);

        } catch (NoSuchFieldException | SecurityException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Collector> T safeRegister(CollectorRegistry registry, T collector) {
        try {
            registry.register(collector);
            return collector;
        } catch (IllegalArgumentException e) {
            // Collector is already registered. Return the existing instance
            try {
                Map<String, Collector> collectorsMap = (Map<String, Collector>) collectorsMapField.get(registry);
                List<String> collectorNames = (List<String>) collectorsNamesMethod.invoke(registry, collector);
                return (T) collectorsMap.get(collectorNames.get(0));

            } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e1) {
                throw new RuntimeException(e1);
            }
        }
    }
}
