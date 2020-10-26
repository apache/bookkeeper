/*
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

package org.apache.bookkeeper.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.bookkeeper.stats.codahale.CodahaleMetricsProvider;
import org.apache.bookkeeper.stats.codahale.FastCodahaleMetricsProvider;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Microbenchmarks for different stats backend providers.
 */
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Threads(16)
@Fork(1)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
public class StatsLoggerBenchmark {

    private static Map<String, Supplier<StatsProvider>> providers = new HashMap<>();

    static {
        providers.put("Prometheus", PrometheusMetricsProvider::new);
        providers.put("Codahale", CodahaleMetricsProvider::new);
        providers.put("FastCodahale", FastCodahaleMetricsProvider::new);
    }

    @State(Scope.Benchmark)
    public static class LoggerState {
        @Param({ "Prometheus", "Codahale", "FastCodahale", "Twitter", "Ostrich" })
        private String statsProvider;

        private Counter counter;
        private OpStatsLogger opStats;

        private long startTime = System.nanoTime();

        @Setup(Level.Trial)
        public void setup() {
            StatsProvider provider = providers.get(statsProvider).get();
            StatsLogger logger = provider.getStatsLogger("test");
            counter = logger.getCounter("counter");
            opStats = logger.getOpStatsLogger("opstats");
        }
    }

     @Benchmark
    public void counterIncrement(LoggerState s) {
        s.counter.inc();
    }

    @Benchmark
    public void recordLatency(LoggerState s) {
        s.opStats.registerSuccessfulValue(System.nanoTime() - s.startTime);
    }
}
