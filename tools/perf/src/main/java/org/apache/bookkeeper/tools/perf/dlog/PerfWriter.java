/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.tools.perf.dlog;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.Unpooled;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.perf.utils.PaddingDecimalFormat;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;

/**
 * A perf writer to evaluate write performance.
 */
@Slf4j
public class PerfWriter implements Runnable {

    /**
     * Flags for the write command.
     */
    public static class Flags extends CliFlags {

        @Parameter(
            names = {
                "-r", "--rate"
            },
            description = "Write rate bytes/s across log streams")
        public int writeRate = 0;

        @Parameter(
            names = {
                "-rs", "--record-size"
            },
            description = "Log record size")
        public int recordSize = 1024;

        @Parameter(
            names = {
                "-ln", "--log-name"
            },
            description = "Log name or log name pattern if more than 1 log is specified at `--num-logs`")
        public String logName = "test-log-%06d";

        @Parameter(
            names = {
                "-l", "--num-logs"
            },
            description = "Number of log streams")
        public int numLogs = 1;

        @Parameter(
            names = {
                "-t", "--threads"
            },
            description = "Number of threads writing")
        public int numThreads = 1;

        @Parameter(
            names = {
                "-mob", "--max-outstanding-megabytes"
            },
            description = "Number of threads writing")
        public long maxOutstandingMB = 200;

        @Parameter(
            names = {
                "-n", "--num-records"
            },
            description = "Number of records to write in total. If 0, it will keep writing")
        public long numRecords = 0;

        @Parameter(
            names = {
                "-mlss", "--max-log-segment-size"
            },
            description = "Max log segment size")
        public int maxLogSegmentSize = 64 * 1024 * 1024;

        @Parameter(
            names = {
                "-b", "--num-bytes"
            },
            description = "Number of bytes to write in total. If 0, it will keep writing")
        public long numBytes = 0;

        @Parameter(
            names = {
                "-e", "--ensemble-size"
            },
            description = "Ledger ensemble size")
        public int ensembleSize = 1;

        @Parameter(
            names = {
                "-w", "--write-quorum-size"
            },
            description = "Ledger write quorum size")
        public int writeQuorumSize = 1;

        @Parameter(
            names = {
                "-a", "--ack-quorum-size"
            },
            description = "Ledger ack quorum size")
        public int ackQuorumSize = 1;

    }


    // stats
    private final LongAdder recordsWritten = new LongAdder();
    private final LongAdder bytesWritten = new LongAdder();

    private final byte[] payload;
    private final ServiceURI serviceURI;
    private final Flags flags;
    private final Recorder recorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );
    private final Recorder cumulativeRecorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );
    private final AtomicBoolean isDone = new AtomicBoolean(false);

    PerfWriter(ServiceURI serviceURI, Flags flags) {
        this.serviceURI = serviceURI;
        this.flags = flags;
        this.payload = new byte[flags.recordSize];
        ThreadLocalRandom.current().nextBytes(payload);
    }

    @Override
    public void run() {
        try {
            execute();
        } catch (Exception e) {
            log.error("Encountered exception at running dlog perf writer", e);
        }
    }

    void execute() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info("Starting dlog perf writer with config : {}", w.writeValueAsString(flags));

        DistributedLogConfiguration conf = newDlogConf(flags);
        try (Namespace namespace = NamespaceBuilder.newBuilder()
             .conf(conf)
             .uri(serviceURI.getUri())
             .build()) {
            execute(namespace);
        }
    }

    void execute(Namespace namespace) throws Exception {
        List<Pair<Integer, DistributedLogManager>> managers = new ArrayList<>(flags.numLogs);
        for (int i = 0; i < flags.numLogs; i++) {
            String logName = String.format(flags.logName, i);
            managers.add(Pair.of(i, namespace.openLog(logName)));
        }
        log.info("Successfully open {} logs", managers.size());

        // register shutdown hook to aggregate stats
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isDone.set(true);
            printAggregatedStats(cumulativeRecorder);
        }));

        ExecutorService executor = Executors.newFixedThreadPool(flags.numThreads);
        try {
            for (int i = 0; i < flags.numThreads; i++) {
                final int idx = i;
                final List<DistributedLogManager> logsThisThread = managers
                    .stream()
                    .filter(pair -> pair.getLeft() % flags.numThreads == idx)
                    .map(pair -> pair.getRight())
                    .collect(Collectors.toList());
                final long numRecordsForThisThread = flags.numRecords / flags.numThreads;
                final long numBytesForThisThread = flags.numBytes / flags.numThreads;
                final double writeRateForThisThread = flags.writeRate / (double) flags.numThreads;
                final long maxOutstandingBytesForThisThread = flags.maxOutstandingMB * 1024 * 1024 / flags.numThreads;
                executor.submit(() -> {
                    try {
                        write(
                            logsThisThread,
                            writeRateForThisThread,
                            (int) maxOutstandingBytesForThisThread,
                            numRecordsForThisThread,
                            numBytesForThisThread);
                    } catch (Exception e) {
                        log.error("Encountered error at writing records", e);
                    }
                });
            }
            log.info("Started {} write threads", flags.numThreads);
            reportStats();
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
            managers.forEach(manager -> manager.getRight().asyncClose());
        }
    }

    void write(List<DistributedLogManager> logs,
               double writeRate,
               int maxOutstandingBytesForThisThread,
               long numRecordsForThisThread,
               long numBytesForThisThread) throws Exception {
        log.info("Write thread started with : logs = {}, rate = {},"
                + " num records = {}, num bytes = {}, max outstanding bytes = {}",
            logs.stream().map(l -> l.getStreamName()).collect(Collectors.toList()),
            writeRate,
            numRecordsForThisThread,
            numBytesForThisThread,
            maxOutstandingBytesForThisThread);

        List<CompletableFuture<AsyncLogWriter>> writerFutures = logs.stream()
            .map(manager -> manager.openAsyncLogWriter())
            .collect(Collectors.toList());
        List<AsyncLogWriter> writers = result(FutureUtils.collect(writerFutures));

        long txid = writers
            .stream()
            .mapToLong(writer -> writer.getLastTxId())
            .max()
            .orElse(0L);
        txid = Math.max(0L, txid);

        RateLimiter limiter;
        if (writeRate > 0) {
            limiter = RateLimiter.create(writeRate);
        } else {
            limiter = null;
        }
        final Semaphore semaphore;
        if (maxOutstandingBytesForThisThread > 0) {
            semaphore = new Semaphore(maxOutstandingBytesForThisThread);
        } else {
            semaphore = null;
        }

        // Acquire 1 second worth of records to have a slower ramp-up
        if (limiter != null) {
            limiter.acquire((int) writeRate);
        }

        long totalWritten = 0L;
        long totalBytesWritten = 0L;
        final int numLogs = logs.size();
        while (true) {
            for (int i = 0; i < numLogs; i++) {
                if (numRecordsForThisThread > 0
                    && totalWritten >= numRecordsForThisThread) {
                    markPerfDone();
                }
                if (numBytesForThisThread > 0
                    && totalBytesWritten >= numBytesForThisThread) {
                    markPerfDone();
                }
                if (null != semaphore) {
                    semaphore.acquire(payload.length);
                }

                totalWritten++;
                totalBytesWritten += payload.length;
                if (null != limiter) {
                    limiter.acquire(payload.length);
                }
                final long sendTime = System.nanoTime();
                writers.get(i).write(
                    new LogRecord(++txid, Unpooled.wrappedBuffer(payload))
                ).thenAccept(dlsn -> {
                    if (null != semaphore) {
                        semaphore.release(payload.length);
                    }

                    recordsWritten.increment();
                    bytesWritten.add(payload.length);

                    long latencyMicros = TimeUnit.NANOSECONDS.toMicros(
                        System.nanoTime() - sendTime
                    );
                    recorder.recordValue(latencyMicros);
                    cumulativeRecorder.recordValue(latencyMicros);
                }).exceptionally(cause -> {
                    log.warn("Error at writing records", cause);
                    System.exit(-1);
                    return null;
                });
            }
        }
    }

    @SuppressFBWarnings("DM_EXIT")
    void markPerfDone() throws Exception {
        log.info("------------------- DONE -----------------------");
        printAggregatedStats(cumulativeRecorder);
        isDone.set(true);
        Thread.sleep(5000);
        System.exit(0);
    }

    void reportStats() {
        // Print report stats
        long oldTime = System.nanoTime();

        Histogram reportHistogram = null;

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }

            if (isDone.get()) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;

            double rate = recordsWritten.sumThenReset() / elapsed;
            double throughput = bytesWritten.sumThenReset() / elapsed / 1024 / 1024;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);

            log.info(
                    "Throughput written : {}  records/s --- {} MB/s --- Latency: mean:"
                        + " {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
                    throughputFormat.format(rate), throughputFormat.format(throughput),
                    dec.format(reportHistogram.getMean() / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0),
                    dec.format(reportHistogram.getMaxValue() / 1000.0));

            reportHistogram.reset();

            oldTime = now;
        }

    }

    private static DistributedLogConfiguration newDlogConf(Flags flags) {
        return new DistributedLogConfiguration()
            .setEnsembleSize(flags.ensembleSize)
            .setWriteQuorumSize(flags.writeQuorumSize)
            .setAckQuorumSize(flags.ackQuorumSize)
            .setOutputBufferSize(512 * 1024)
            .setPeriodicFlushFrequencyMilliSeconds(2)
            .setWriteLockEnabled(false)
            .setMaxLogSegmentBytes(flags.maxLogSegmentSize)
            .setLogSegmentRollingIntervalMinutes(1)
            .setExplicitTruncationByApplication(true);
    }


    private static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.0", 8);
    private static final DecimalFormat dec = new PaddingDecimalFormat("0.000", 7);

    private static void printAggregatedStats(Recorder recorder) {
        Histogram reportHistogram = recorder.getIntervalHistogram();

        log.info("Aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {}"
                + " - 99.9pct: {} - 99.99pct: {} - 99.999pct: {} - Max: {}",
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
