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

import com.google.common.annotations.VisibleForTesting;

import io.netty.util.concurrent.DefaultThreadFactory;
//CHECKSTYLE.OFF: IllegalImport
import io.netty.util.internal.PlatformDependent;
//CHECKSTYLE.ON: IllegalImport
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Gauge.Child;
import io.prometheus.client.hotspot.GarbageCollectorExports;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import io.prometheus.client.hotspot.StandardExports;
import io.prometheus.client.hotspot.ThreadExports;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.stats.CachingStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A <i>Prometheus</i> based {@link StatsProvider} implementation.
 */
public class PrometheusMetricsProvider implements StatsProvider {

    private ScheduledExecutorService executor;

    public static final String PROMETHEUS_STATS_HTTP_ENABLE = "prometheusStatsHttpEnable";
    public static final boolean DEFAULT_PROMETHEUS_STATS_HTTP_ENABLE = true;

    public static final String PROMETHEUS_STATS_HTTP_PORT = "prometheusStatsHttpPort";
    public static final int DEFAULT_PROMETHEUS_STATS_HTTP_PORT = 8000;

    public static final String PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS = "prometheusStatsLatencyRolloverSeconds";
    public static final int DEFAULT_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS = 60;

    final CollectorRegistry registry;

    Server server;
    private final CachingStatsProvider cachingStatsProvider;

    /*
     * These acts a registry of the metrics defined in this provider
     */
    final ConcurrentMap<String, LongAdderCounter> counters = new ConcurrentSkipListMap<>();
    final ConcurrentMap<String, SimpleGauge<? extends Number>> gauges = new ConcurrentSkipListMap<>();
    final ConcurrentMap<String, DataSketchesOpStatsLogger> opStats = new ConcurrentSkipListMap<>();

    public PrometheusMetricsProvider() {
        this(CollectorRegistry.defaultRegistry);
    }

    public PrometheusMetricsProvider(CollectorRegistry registry) {
        this.registry = registry;
        this.cachingStatsProvider = new CachingStatsProvider(new StatsProvider() {
            @Override
            public void start(Configuration conf) {
                // nop
            }

            @Override
            public void stop() {
                // nop
            }

            @Override
            public StatsLogger getStatsLogger(String scope) {
                return new PrometheusStatsLogger(PrometheusMetricsProvider.this, scope);
            }

            @Override
            public String getStatsName(String... statsComponents) {
                String completeName;
                if (statsComponents.length == 0) {
                    return "";
                } else if (statsComponents[0].isEmpty()) {
                    completeName = StringUtils.join(statsComponents, '_', 1, statsComponents.length);
                } else {
                    completeName = StringUtils.join(statsComponents, '_');
                }
                return Collector.sanitizeMetricName(completeName);
            }
        });
    }

    @Override
    public void start(Configuration conf) {
        boolean httpEnabled = conf.getBoolean(PROMETHEUS_STATS_HTTP_ENABLE, DEFAULT_PROMETHEUS_STATS_HTTP_ENABLE);
        boolean bkHttpServerEnabled = conf.getBoolean("httpServerEnabled", false);
        // only start its own http server when prometheus http is enabled and bk http server is not enabled.
        if (httpEnabled && !bkHttpServerEnabled) {
            int httpPort = conf.getInt(PROMETHEUS_STATS_HTTP_PORT, DEFAULT_PROMETHEUS_STATS_HTTP_PORT);
            InetSocketAddress httpEndpoint = InetSocketAddress.createUnresolved("0.0.0.0", httpPort);
            this.server = new Server(httpEndpoint);
            ServletContextHandler context = new ServletContextHandler();
            context.setContextPath("/");
            server.setHandler(context);

            context.addServlet(new ServletHolder(new PrometheusServlet(this)), "/metrics");

            try {
                server.start();
                log.info("Started Prometheus stats endpoint at {}", httpEndpoint);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Include standard JVM stats
        registerMetrics(new StandardExports());
        registerMetrics(new MemoryPoolsExports());
        registerMetrics(new GarbageCollectorExports());
        registerMetrics(new ThreadExports());

        // Add direct memory allocated through unsafe
        registerMetrics(Gauge.build("jvm_memory_direct_bytes_used", "-").create().setChild(new Child() {
            @Override
            public double get() {
                return directMemoryUsage != null ? directMemoryUsage.longValue() : Double.NaN;
            }
        }));

        registerMetrics(Gauge.build("jvm_memory_direct_bytes_max", "-").create().setChild(new Child() {
            @Override
            public double get() {
                return PlatformDependent.maxDirectMemory();
            }
        }));

        executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("metrics"));

        int latencyRolloverSeconds = conf.getInt(PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS,
                DEFAULT_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS);

        executor.scheduleAtFixedRate(() -> {
            rotateLatencyCollection();
        }, 1, latencyRolloverSeconds, TimeUnit.SECONDS);

    }

    @Override
    public void stop() {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                log.warn("Failed to shutdown Jetty server", e);
            }
        }
    }

    @Override
    public StatsLogger getStatsLogger(String scope) {
        return this.cachingStatsProvider.getStatsLogger(scope);
    }

    @Override
    public void writeAllMetrics(Writer writer) throws IOException {
        PrometheusTextFormatUtil.writeMetricsCollectedByPrometheusClient(writer, registry);

        gauges.forEach((name, gauge) -> PrometheusTextFormatUtil.writeGauge(writer, name, gauge));
        counters.forEach((name, counter) -> PrometheusTextFormatUtil.writeCounter(writer, name, counter));
        opStats.forEach((name, opStatLogger) -> PrometheusTextFormatUtil.writeOpStat(writer, name, opStatLogger));
    }

    @Override
    public String getStatsName(String... statsComponents) {
        return cachingStatsProvider.getStatsName(statsComponents);
    }

    @VisibleForTesting
    void rotateLatencyCollection() {
        opStats.forEach((name, metric) -> {
            metric.rotateLatencyCollection();
        });
    }

    private void registerMetrics(Collector collector) {
        try {
            collector.register(registry);
        } catch (Exception e) {
            // Ignore if these were already registered
            if (log.isDebugEnabled()) {
                log.debug("Failed to register Prometheus collector exports", e);
            }
        }
    }


    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsProvider.class);

    /*
     * Try to get Netty counter of used direct memory. This will be correct, unlike the JVM values.
     */
    private static final AtomicLong directMemoryUsage;
    static {
        AtomicLong tmpDirectMemoryUsage = null;

        try {
            Field field = PlatformDependent.class.getDeclaredField("DIRECT_MEMORY_COUNTER");
            field.setAccessible(true);
            tmpDirectMemoryUsage = (AtomicLong) field.get(null);
        } catch (Throwable t) {
            log.warn("Failed to access netty DIRECT_MEMORY_COUNTER field {}", t.getMessage());
        }

        directMemoryUsage = tmpDirectMemoryUsage;
    }
}
