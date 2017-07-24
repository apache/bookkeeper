/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.stats.twitter.science;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Supplier;
import com.twitter.common.net.http.handlers.VarsHandler;
import com.twitter.common.net.http.handlers.VarsJsonHandler;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.JvmStats;
import com.twitter.common.stats.Stat;
import com.twitter.common.stats.Stats;
import com.twitter.common.stats.TimeSeriesRepository;
import com.twitter.common.stats.TimeSeriesRepositoryImpl;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Starts a jetty server on a configurable port and the samplers to export stats.
 */
public class HTTPStatsExporter {
    final Server jettyServer;
    final ShutdownRegistry.ShutdownRegistryImpl shutDownRegistry;
    final int port;

    public HTTPStatsExporter(int port) {
        // Create the ShutdownRegistry needed for our sampler
        this.shutDownRegistry = new ShutdownRegistry.ShutdownRegistryImpl();
        this.port = port;
        this.jettyServer = new Server(port);
    }

    public void start() throws Exception {
        // Start the sampler. Sample every 1 second and retain for 1 hour
        TimeSeriesRepository sampler = new TimeSeriesRepositoryImpl(Stats.STAT_REGISTRY,
                Amount.of(1L, Time.SECONDS), Amount.of(1L, Time.HOURS));
        sampler.start(this.shutDownRegistry);
        // Export JVM stats
        JvmStats.export();
        // Configure handlers
        Supplier<Iterable<Stat<?>>> supplier = new Supplier<Iterable<Stat<?>>>() {
            @Override
            public Iterable<Stat<?>> get() {
                return Stats.getVariables();
            }
        };

        // Start jetty.
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        this.jettyServer.setHandler(context);
        context.addServlet(new ServletHolder(new VarsHandler(supplier)), "/vars");
        context.addServlet(new ServletHolder(new VarsJsonHandler(supplier)), "/vars.json");
        this.jettyServer.start();
    }

    public void stop() throws Exception {
        this.jettyServer.stop();
        if (this.shutDownRegistry != null) {
            this.shutDownRegistry.execute();
        }
    }
}
