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

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.perf.utils.PaddingDecimalFormat;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;

/**
 * A perf writer to evaluate write performance.
 */
@Slf4j
public class PerfReader implements Runnable {

    /**
     * Flags for the write command.
     */
    public static class Flags extends CliFlags {

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
            description = "Number of threads reading")
        public int numThreads = 1;

        @Parameter(
            names = {
                "-mr", "--max-readahead-records"
            },
            description = "Max readhead records")
        public int maxReadAheadRecords = 1000000;

        @Parameter(
            names = {
                "-bs", "--readahead-batch-size"
            },
            description = "ReadAhead Batch Size, in entries"
        )
        public int readAheadBatchSize = 4;

    }


    // stats
    private final LongAdder recordsRead = new LongAdder();
    private final LongAdder bytesRead = new LongAdder();

    private final ServiceURI serviceURI;
    private final Flags flags;
    private final Recorder recorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );
    private final Recorder cumulativeRecorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );
    private final AtomicBoolean isDone = new AtomicBoolean(false);

    PerfReader(ServiceURI serviceURI, Flags flags) {
        this.serviceURI = serviceURI;
        this.flags = flags;
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
        log.info("Starting dlog perf reader with config : {}", w.writeValueAsString(flags));

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
                executor.submit(() -> {
                    try {
                        read(logsThisThread);
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

    void read(List<DistributedLogManager> logs) throws Exception {
        log.info("Read thread started with : logs = {}",
            logs.stream().map(l -> l.getStreamName()).collect(Collectors.toList()));

        List<LogReader> readers = logs.stream()
            .map(manager -> {
                try {
                    return manager.openLogReader(DLSN.InitialDLSN);
                } catch (IOException e) {
                    log.error("Failed to open reader for log stream {}", manager.getStreamName(), e);
                    throw new UncheckedIOException(e);
                }
            })
            .collect(Collectors.toList());

        final int numLogs = logs.size();
        while (true) {
            for (int i = 0; i < numLogs; i++) {
                LogRecordWithDLSN record = readers.get(i).readNext(true);
                if (null != record) {
                    recordsRead.increment();
                    bytesRead.add(record.getPayloadBuf().readableBytes());
                }
            }
        }
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

            double rate = recordsRead.sumThenReset() / elapsed;
            double throughput = bytesRead.sumThenReset() / elapsed / 1024 / 1024;

            reportHistogram = recorder.getIntervalHistogram(reportHistogram);

            log.info("Throughput read : {}  records/s --- {} MB/s --- Latency: mean:"
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
            .setReadAheadBatchSize(flags.readAheadBatchSize)
            .setReadAheadMaxRecords(flags.maxReadAheadRecords)
            .setReadAheadWaitTime(200);
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
