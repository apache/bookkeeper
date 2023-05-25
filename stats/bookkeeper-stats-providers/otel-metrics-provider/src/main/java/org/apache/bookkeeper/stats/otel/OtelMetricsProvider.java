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

// CHECKSTYLE.OFF: IllegalImport
import io.netty.util.internal.PlatformDependent;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.instrumentation.runtimemetrics.BufferPools;
import io.opentelemetry.instrumentation.runtimemetrics.Classes;
import io.opentelemetry.instrumentation.runtimemetrics.Cpu;
import io.opentelemetry.instrumentation.runtimemetrics.GarbageCollector;
import io.opentelemetry.instrumentation.runtimemetrics.MemoryPools;
import io.opentelemetry.instrumentation.runtimemetrics.Threads;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
// CHECKSTYLE.ON: IllegalImport

@Slf4j
public class OtelMetricsProvider implements StatsProvider {

    private static final String METER_NAME = "org.apache.bookkeeper";

    /*
     * These acts a registry of the metrics defined in this provider
     */
    final ConcurrentMap<ScopeContext, OtelCounter> counters = new ConcurrentHashMap<>();
    final ConcurrentMap<ScopeContext, OtelOpStatsLogger> opStats = new ConcurrentHashMap<>();

    private static final List<Double> histogramBuckets = Arrays.asList(
            0.1, 0.2, 0.5,
            1.0, 2.0, 5.0,
            10.0, 20.0, 50.0,
            100.0, 200.0, 500.0,
            1_000.0, 2_000.0, 5_000.0,
            10_000.0, 20_000.0, 50_000.0
    );

    private final OpenTelemetry openTelemetry;

    final Meter meter;

    OtelMetricsProvider() {
        AutoConfiguredOpenTelemetrySdk sdk = AutoConfiguredOpenTelemetrySdk.builder()
                .addMeterProviderCustomizer(
                        (sdkMeterProviderBuilder, configProperties) ->
                                sdkMeterProviderBuilder.registerView(
                                        InstrumentSelector.builder()
                                                .setMeterName(METER_NAME)
                                                .setType(InstrumentType.HISTOGRAM)
                                                .build(),
                                        View.builder()
                                                .setAggregation(Aggregation.explicitBucketHistogram(histogramBuckets))
                                                .build())

                ).build();
        this.openTelemetry = sdk.getOpenTelemetrySdk();
        this.meter = openTelemetry.getMeter(METER_NAME);
    }

    @Override
    public void start(Configuration conf) {
        boolean exposeDefaultJVMMetrics = conf.getBoolean("exposeDefaultJVMMetrics", true);
        if (exposeDefaultJVMMetrics) {
            // Include standard JVM stats
            MemoryPools.registerObservers(openTelemetry);
            BufferPools.registerObservers(openTelemetry);
            Classes.registerObservers(openTelemetry);
            Cpu.registerObservers(openTelemetry);
            Threads.registerObservers(openTelemetry);
            GarbageCollector.registerObservers(openTelemetry);

            meter.gaugeBuilder("process.runtime.jvm.memory.direct_bytes_used")
                    .buildWithCallback(odm -> odm.record(getDirectMemoryUsage.get()));

            meter.gaugeBuilder("process.runtime.jvm.memory.direct_bytes_max")
                    .buildWithCallback(odm -> odm.record(PlatformDependent.estimateMaxDirectMemory()));
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public StatsLogger getStatsLogger(String scope) {
        return new OtelStatsLogger(this, scope, Attributes.empty());
    }

    /*
     * Try to get Netty counter of used direct memory. This will be correct, unlike the JVM values.
     */
    private static final AtomicLong directMemoryUsage;
    private static final Optional<BufferPoolMXBean> poolMxBeanOp;
    private static final Supplier<Double> getDirectMemoryUsage;

    static {
        if (PlatformDependent.useDirectBufferNoCleaner()) {
            poolMxBeanOp = Optional.empty();
            AtomicLong tmpDirectMemoryUsage = null;
            try {
                Field field = PlatformDependent.class.getDeclaredField("DIRECT_MEMORY_COUNTER");
                field.setAccessible(true);
                tmpDirectMemoryUsage = (AtomicLong) field.get(null);
            } catch (Throwable t) {
                log.warn("Failed to access netty DIRECT_MEMORY_COUNTER field {}", t.getMessage());
            }
            directMemoryUsage = tmpDirectMemoryUsage;
            getDirectMemoryUsage = () -> directMemoryUsage != null ? directMemoryUsage.get() : Double.NaN;
        } else {
            directMemoryUsage = null;
            List<BufferPoolMXBean> platformMXBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            poolMxBeanOp = platformMXBeans.stream()
                    .filter(bufferPoolMXBean -> bufferPoolMXBean.getName().equals("direct")).findAny();
            getDirectMemoryUsage = () -> poolMxBeanOp.isPresent() ? poolMxBeanOp.get().getMemoryUsed() : Double.NaN;
        }
    }
}
