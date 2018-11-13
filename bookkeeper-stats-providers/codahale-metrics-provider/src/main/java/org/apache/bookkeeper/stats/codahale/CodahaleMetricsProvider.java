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
package org.apache.bookkeeper.stats.codahale;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StatsProvider} implemented based on <i>Codahale</i> metrics library.
 */
@SuppressWarnings("deprecation")
public class CodahaleMetricsProvider implements StatsProvider {

    static final Logger LOG = LoggerFactory.getLogger(CodahaleMetricsProvider.class);

    MetricRegistry metrics = null;
    List<ScheduledReporter> reporters = new ArrayList<ScheduledReporter>();
    JmxReporter jmx = null;

    synchronized void initIfNecessary() {
        if (metrics == null) {
            metrics = new MetricRegistry();
            metrics.registerAll(new MemoryUsageGaugeSet());
            metrics.registerAll(new GarbageCollectorMetricSet());
        }
    }

    public synchronized MetricRegistry getMetrics() {
        return metrics;
    }

    @Override
    public void start(Configuration conf) {
        initIfNecessary();

        int metricsOutputFrequency = conf.getInt("codahaleStatsOutputFrequencySeconds", 60);
        String prefix = conf.getString("codahaleStatsPrefix", "");
        String graphiteHost = conf.getString("codahaleStatsGraphiteEndpoint");
        String csvDir = conf.getString("codahaleStatsCSVEndpoint");
        String slf4jCat = conf.getString("codahaleStatsSlf4jEndpoint");
        String jmxDomain = conf.getString("codahaleStatsJmxEndpoint");

        if (!Strings.isNullOrEmpty(graphiteHost)) {
            LOG.info("Configuring stats with graphite");
            HostAndPort addr = HostAndPort.fromString(graphiteHost);
            final Graphite graphite = new Graphite(
                    new InetSocketAddress(addr.getHostText(), addr.getPort()));
            reporters.add(GraphiteReporter.forRegistry(getMetrics())
                          .prefixedWith(prefix)
                          .convertRatesTo(TimeUnit.SECONDS)
                          .convertDurationsTo(TimeUnit.MILLISECONDS)
                          .filter(MetricFilter.ALL)
                          .build(graphite));
        }
        if (!Strings.isNullOrEmpty(csvDir)) {
            // NOTE: 1/ metrics output files are exclusive to a given process
            // 2/ the output directory must exist
            // 3/ if output files already exist they are not overwritten and there is no metrics output
            File outdir;
            if (!Strings.isNullOrEmpty(prefix)) {
                outdir = new File(csvDir, prefix);
            } else {
                outdir = new File(csvDir);
            }
            LOG.info("Configuring stats with csv output to directory [{}]", outdir.getAbsolutePath());
            reporters.add(CsvReporter.forRegistry(getMetrics())
                          .convertRatesTo(TimeUnit.SECONDS)
                          .convertDurationsTo(TimeUnit.MILLISECONDS)
                          .build(outdir));
        }
        if (!Strings.isNullOrEmpty(slf4jCat)) {
            LOG.info("Configuring stats with slf4j");
            reporters.add(Slf4jReporter.forRegistry(getMetrics())
                          .outputTo(LoggerFactory.getLogger(slf4jCat))
                          .convertRatesTo(TimeUnit.SECONDS)
                          .convertDurationsTo(TimeUnit.MILLISECONDS)
                          .build());
        }
        if (!Strings.isNullOrEmpty(jmxDomain)) {
            LOG.info("Configuring stats with jmx");
            jmx = JmxReporter.forRegistry(getMetrics())
                .inDomain(jmxDomain)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
            jmx.start();
        }

        for (ScheduledReporter r : reporters) {
            r.start(metricsOutputFrequency, TimeUnit.SECONDS);
        }
    }

    @Override
    public void stop() {
        for (ScheduledReporter r : reporters) {
            r.report();
            r.stop();
        }
        if (jmx != null) {
            jmx.stop();
        }
    }

    @Override
    public StatsLogger getStatsLogger(String name) {
        initIfNecessary();
        return new CodahaleStatsLogger(getMetrics(), name);
    }

    @Override
    public String getStatsName(String... statsComponents) {
        if (statsComponents.length == 0) {
            return "";
        }
        String baseName = statsComponents[0];
        String[] names = new String[statsComponents.length - 1];
        System.arraycopy(statsComponents, 1, names, 0, names.length);
        return MetricRegistry.name(baseName, names);
    }
}
