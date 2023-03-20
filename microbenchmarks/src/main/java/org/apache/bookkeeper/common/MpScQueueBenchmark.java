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

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.bookkeeper.common.collections.BatchedArrayBlockingQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Microbenchmarks for different executors providers.
 */
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.Throughput)
@Threads(16)
@Fork(1)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
public class MpScQueueBenchmark {

    private static final int QUEUE_SIZE = 100_000;

    /**
     * State holder of the test.
     */
    @State(Scope.Benchmark)
    public static class TestState {

        private ArrayBlockingQueue arrayBlockingQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);

        private BatchedArrayBlockingQueue batchedArrayBlockingQueue = new BatchedArrayBlockingQueue<>(QUEUE_SIZE);

        private final Integer[] batchArray = new Integer[1000];

        private final ExecutorService executor = Executors.newCachedThreadPool();

        @Setup(Level.Trial)
        public void setup() {
            for (int i = 0; i < 1000; i++) {
                batchArray[i] = i;
            }

            executor.execute(this::consumeABQ);
            executor.execute(this::consumeBAABQ);
        }

        @SneakyThrows
        private void consumeABQ() {
            ArrayList<Integer> localList = new ArrayList<>();

            try {
                while (true) {
                    arrayBlockingQueue.drainTo(localList);
                    if (localList.isEmpty()) {
                        arrayBlockingQueue.take();
                    }
                    localList.clear();
                }
            } catch (InterruptedException ie) {
            }
        }

        @SneakyThrows
        private void consumeBAABQ() {
            Integer[] localArray = new Integer[20_000];

            try {
                while (true) {
                    batchedArrayBlockingQueue.takeAll(localArray);
                }
            } catch (InterruptedException ie) {
            }
        }

        @TearDown(Level.Trial)
        public void teardown() {
            executor.shutdownNow();
        }

        @TearDown(Level.Iteration)
        public void cleanupQueue() throws InterruptedException{
            Thread.sleep(1_000);
        }
    }

    @Benchmark
    public void arrayBlockingQueue(TestState s) throws Exception {
        s.arrayBlockingQueue.put(1);
    }

    @Benchmark
    public void batchAwareArrayBlockingQueueSingleEnqueue(TestState s) throws Exception {
        s.batchedArrayBlockingQueue.put(1);
    }

    @Benchmark
    @OperationsPerInvocation(1000)
    public void batchAwareArrayBlockingQueueBatch(TestState s) throws Exception {
        s.batchedArrayBlockingQueue.putAll(s.batchArray, 0, 1000);
    }
}
