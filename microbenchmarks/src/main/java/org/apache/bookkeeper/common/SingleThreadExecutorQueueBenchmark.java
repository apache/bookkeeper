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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.bookkeeper.common.collections.BatchedArrayBlockingQueue;
import org.apache.bookkeeper.common.collections.BatchedBlockingQueue;
import org.apache.bookkeeper.common.collections.GrowableBatchedArrayBlockingQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares the queue options for {@code SingleThreadExecutor}: the JDK {@code ArrayBlockingQueue}
 * drained with {@code drainTo}, and the batched queues ({@code BatchedArrayBlockingQueue} and
 * {@code GrowableBatchedArrayBlockingQueue}) drained with {@code takeAll}.
 *
 * <p>The consumer threads mirror the drain patterns of {@code SingleThreadExecutor.run()}.
 */
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.Throughput)
@Threads(8)
@Fork(1)
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
public class SingleThreadExecutorQueueBenchmark {

    private static final int QUEUE_SIZE = 100_000;

    /**
     * State holder of the test.
     */
    @State(Scope.Benchmark)
    public static class TestState {

        private final ArrayBlockingQueue<Integer> arrayBlockingQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);

        private final BatchedArrayBlockingQueue<Integer> batchedQueue = new BatchedArrayBlockingQueue<>(QUEUE_SIZE);

        private final GrowableBatchedArrayBlockingQueue<Integer> growableBatchedQueue =
                new GrowableBatchedArrayBlockingQueue<>();

        private final ExecutorService executor = Executors.newCachedThreadPool();

        @Setup(Level.Trial)
        public void setup() {
            executor.execute(() -> consumeWithDrainTo(arrayBlockingQueue));
            executor.execute(() -> consumeWithTakeAll(batchedQueue));
            executor.execute(() -> consumeWithTakeAll(growableBatchedQueue));
        }

        @SneakyThrows
        private void consumeWithDrainTo(BlockingQueue<Integer> queue) {
            List<Integer> localTasks = new ArrayList<>();

            try {
                while (true) {
                    int n = queue.drainTo(localTasks);
                    if (n > 0) {
                        localTasks.clear();
                    } else {
                        queue.take();
                    }
                }
            } catch (InterruptedException ie) {
            }
        }

        @SneakyThrows
        private void consumeWithTakeAll(BatchedBlockingQueue<Integer> queue) {
            Integer[] localTasks = new Integer[20_000];

            try {
                while (true) {
                    queue.takeAll(localTasks);
                }
            } catch (InterruptedException ie) {
            }
        }

        @TearDown(Level.Trial)
        public void teardown() {
            executor.shutdownNow();
        }

        @TearDown(Level.Iteration)
        public void drainBacklog() throws InterruptedException {
            Thread.sleep(1_000);
        }
    }

    @Benchmark
    public void arrayBlockingQueue(TestState s) throws Exception {
        s.arrayBlockingQueue.put(1);
    }

    @Benchmark
    public void batchedArrayBlockingQueue(TestState s) throws Exception {
        s.batchedQueue.put(1);
    }

    @Benchmark
    public void growableBatchedArrayBlockingQueue(TestState s) throws Exception {
        s.growableBatchedQueue.put(1);
    }
}
