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
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.GarbageCollectorExports;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import io.prometheus.client.hotspot.StandardExports;
import io.prometheus.client.hotspot.ThreadExports;
import java.net.InetSocketAddress;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A <i>Prometheus</i> based {@link StatsProvider} implementation.
 */
public class PrometheusMetricsProvider implements StatsProvider {

    private final CollectorRegistry registry = new CollectorRegistry();
    private Server server;

    @Override
    public void start(Configuration conf) {
        int httpPort = conf.getInt("prometheusStatsHttpPort", 8000);
        InetSocketAddress httpEndpoint = InetSocketAddress.createUnresolved("0.0.0.0", httpPort);
        this.server = new Server(httpEndpoint);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);

        context.addServlet(new ServletHolder(new MetricsServlet(registry)), "/metrics");

        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        log.info("Started Prometheus stats endpoint at {}", httpEndpoint);

        // Include standard JVM stats
        new StandardExports().register(registry);
        new MemoryPoolsExports().register(registry);
        new GarbageCollectorExports().register(registry);
        new ThreadExports().register(registry);
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
        return new PrometheusStatsLogger(registry, scope);
    }

    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsProvider.class);
}
