/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.tools.perf.table;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.perf.utils.PaddingDecimalFormat;

/**
 * Perf client to evaluate the performance of table service.
 */
@Slf4j
public class PerfClient implements Runnable {

    enum OP {
        PUT,
        GET,
        INC,
        DEL
    }

    /**
     * Flags for the perf client.
     */
    public static class Flags extends CliFlags {

        @Parameter(
            names = {
                "-r", "--rate"
            },
            description = "Request rate - requests/second")
        public int rate = 100000;

        @Parameter(
            names = {
                "-mor", "--max-outstanding-requests"
            },
            description = "Max outstanding request")
        public int maxOutstandingRequests = 10000;

        @Parameter(
            names = {
                "-ks", "--key-size"
            },
            description = "Key size")
        public int keySize = 16;

        @Parameter(
            names = {
                "-vs", "--value-size"
            },
            description = "Value size")
        public int valueSize = 100;

        @Parameter(
            names = {
                "-t", "--table-name"
            },
            description = "Table name")
        public String tableName = "test-table";

        @Parameter(
            names = {
                "-nk", "--num-keys"
            },
            description = "Number of the keys to test")
        public int numKeys = 1000000;

        @Parameter(
            names = {
                "-kpp", "--keys-per-prefix"
            },
            description = "control average number of keys generated per prefix,"
                + " 0 means no special handling of the prefix, i.e. use the"
                + " prefix comes with the generated random number"
        )
        public int keysPerPrefix = 0;

        @Parameter(
            names = {
                "-ps", "--prefix-size"
            },
            description = "Prefix size"
        )
        public int prefixSize = 0;

        @Parameter(
            names = {
                "-no", "--num-ops"
            },
            description = "Number of client operations to test")
        public int numOps = 0;

        @Parameter(
            names = {
                "-ns", "--namespace"
            },
            description = "Namespace of the tables to benchmark")
        public String namespace = "benchmark";

        @Parameter(
            names = {
                "-b", "--benchmarks"
            },
            description = "List of benchamrks to run")
        public List<String> benchmarks;

    }

    static class OpStats {

        private final String name;
        private final LongAdder ops = new LongAdder();
        private final Recorder recorder = new Recorder(
            TimeUnit.SECONDS.toMillis(120000), 5
        );
        private final Recorder cumulativeRecorder = new Recorder(
            TimeUnit.SECONDS.toMillis(120000), 5
        );
        private Histogram reportHistogram;

        OpStats(String name) {
            this.name = name;
        }

        void recordOp(long latencyMicros) {
            ops.increment();
            recorder.recordValue(latencyMicros);
            cumulativeRecorder.recordValue(latencyMicros);
        }

        void reportStats(long oldTime) {
            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;
            double rate = ops.sumThenReset() / elapsed;
            reportHistogram = recorder.getIntervalHistogram(reportHistogram);
            log.info(
                "[{}] Throughput: {}  ops/s --- Latency: mean:"
                        + " {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
                    name,
                    throughputFormat.format(rate),
                    dec.format(reportHistogram.getMean() / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
                    dec.format(reportHistogram.getMaxValue() / 1000.0));
            reportHistogram.reset();
        }

        void printAggregatedStats() {
            Histogram reportHistogram = cumulativeRecorder.getIntervalHistogram();
            log.info("[{}] latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {}"
                    + " - 99.9pct: {} - 99.99pct: {} - 99.999pct: {} - Max: {}",
                    name,
                    dec.format(reportHistogram.getMean() / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99.999) / 1000.0),
                    dec.format(reportHistogram.getMaxValue() / 1000.0));
        }
    }

    private final ServiceURI serviceURI;
    private final Flags flags;

    PerfClient(ServiceURI serviceURI, Flags flags) {
        this.serviceURI = serviceURI;
        this.flags = flags;
    }

    @Override
    public void run() {
        try {
            execute();
        } catch (Exception e) {
            log.error("Encountered exception at running table perf client", e);
        }
    }

    void execute() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting table perf client with config : {}", w.writeValueAsString(flags));

        runBenchmarkTasks();
    }

    private void runBenchmarkTasks() throws Exception {
        StorageClientSettings settings = StorageClientSettings.newBuilder()
            .serviceUri(serviceURI.getUri().toString())
            .build();
        try (StorageClient client = StorageClientBuilder.newBuilder()
             .withSettings(settings)
             .withNamespace(flags.namespace)
             .build()) {
            try (Table<ByteBuf, ByteBuf> table = result(client.openTable(flags.tableName))) {

                long randSeed = System.currentTimeMillis();
                KeyGenerator generator = new KeyGenerator(flags.numKeys, flags.keysPerPrefix, flags.prefixSize);
                RateLimiter limiter;
                if (flags.rate <= 0) {
                    limiter = null;
                } else {
                    limiter = RateLimiter.create(flags.rate);
                }

                for (String benchmark : flags.benchmarks) {
                    List<BenchmarkTask> tasks = new ArrayList<>();
                    int currentTaskId = 0;
                    Semaphore semaphore;
                    if (flags.maxOutstandingRequests <= 0) {
                        semaphore = null;
                    } else {
                        semaphore = new Semaphore(flags.maxOutstandingRequests);
                    }

                    switch (benchmark) {
                        case "fillseq":
                            tasks.add(new WriteSequentialTask(
                                table,
                                currentTaskId++,
                                randSeed,
                                Math.max(flags.numOps, flags.numKeys),
                                flags.numKeys,
                                flags,
                                generator,
                                limiter,
                                semaphore
                            ));
                            break;
                        case "fillrandom":
                            tasks.add(new WriteRandomTask(
                                table,
                                currentTaskId++,
                                randSeed,
                                Math.max(flags.numOps, flags.numKeys),
                                flags.numKeys,
                                flags,
                                generator,
                                limiter,
                                semaphore
                            ));
                            break;
                        case "incseq":
                            tasks.add(new IncrementSequentialTask(
                                table,
                                currentTaskId++,
                                randSeed,
                                Math.max(flags.numOps, flags.numKeys),
                                flags.numKeys,
                                flags,
                                generator,
                                limiter,
                                semaphore
                            ));
                            break;
                        case "incrandom":
                            tasks.add(new IncrementRandomTask(
                                table,
                                currentTaskId++,
                                randSeed,
                                Math.max(flags.numOps, flags.numKeys),
                                flags.numKeys,
                                flags,
                                generator,
                                limiter,
                                semaphore
                            ));
                            break;
                        default:
                            System.err.println("Unknown benchmark: " + benchmark);
                            break;
                    }

                    if (tasks.isEmpty()) {
                        continue;
                    }

                    final CountDownLatch latch = new CountDownLatch(tasks.size());
                    @Cleanup("shutdown")
                    ExecutorService executor = Executors.newFixedThreadPool(tasks.size());
                    for (BenchmarkTask task : tasks) {
                        executor.submit(() -> {
                            try {
                                task.runTask();
                            } catch (Exception e) {
                                log.error("Encountered issue at running benchmark task {}",
                                    task.tid, e);
                            } finally {
                                latch.countDown();
                            }

                        });
                    }

                    @Cleanup("shutdown")
                    ExecutorService statsExecutor = Executors.newSingleThreadExecutor();
                    statsExecutor.submit(() -> reportStats(tasks));

                    latch.await();

                    log.info("------------------- DONE -----------------------");
                    tasks.forEach(task -> task.printAggregatedStats());
                }
            }
        }
    }

    private void reportStats(List<BenchmarkTask> tasks) {
        long oldTime = System.nanoTime();

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ie) {
                break;
            }

            final long startTime = oldTime;
            tasks.forEach(task -> task.reportStats(startTime));
            oldTime = System.nanoTime();
        }
    }

    private static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.0", 8);
    private static final DecimalFormat dec = new PaddingDecimalFormat("0.000", 7);

}
