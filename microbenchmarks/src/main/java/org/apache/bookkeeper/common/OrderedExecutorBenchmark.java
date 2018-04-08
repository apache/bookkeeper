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

package org.apache.bookkeeper.common;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Microbenchmarks for different executors providers.
 */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(16)
@Fork(1)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
public class OrderedExecutorBenchmark {

    private static Map<String, Supplier<ExecutorService>> providers = ImmutableMap.of( //
            "JDK-ThreadPool", () -> Executors.newFixedThreadPool(1),
            "OrderedExecutor", () -> OrderedExecutor.newBuilder().numThreads(1).build(), //
            "OrderedScheduler", () -> OrderedScheduler.newSchedulerBuilder().numThreads(1).build());

    @State(Scope.Benchmark)
    public static class TestState {
        @Param({ "JDK-ThreadPool", "OrderedExecutor", "OrderedScheduler" })
        private String executorName;

        private ExecutorService executor;

        @Setup(Level.Trial)
        public void setup() {
            executor = providers.get(executorName).get();
        }

        @TearDown(Level.Trial)
        public void teardown() {
            executor.shutdown();
        }
    }

    @Benchmark
    public void submitAndWait(TestState s) throws Exception {
        s.executor.submit(() -> {
        }).get();
    }
}
