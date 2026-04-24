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
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.CustomLog;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.perf.utils.PaddingDecimalFormat;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;

@CustomLog
abstract class PerfReaderBase implements Runnable {

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
                "-ns", "--num-splits-per-segment"
            },
            description = "Num splits per segment")
        public int numSplitsPerSegment = 1;

        @Parameter(
            names = {
                "-bs", "--readahead-batch-size"
            },
            description = "ReadAhead Batch Size, in entries"
        )
        public int readAheadBatchSize = 4;

    }


    // stats
    protected final LongAdder recordsRead = new LongAdder();
    protected final LongAdder bytesRead = new LongAdder();

    protected final ServiceURI serviceURI;
    protected final Flags flags;
    protected final Recorder recorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );
    protected final Recorder cumulativeRecorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );
    protected final AtomicBoolean isDone = new AtomicBoolean(false);

    PerfReaderBase(ServiceURI serviceURI, Flags flags) {
        this.serviceURI = serviceURI;
        this.flags = flags;
    }

    protected void execute() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();
        log.info()
                .attr("config", w.writeValueAsString(flags))
                .log("Starting dlog perf reader");

        DistributedLogConfiguration conf = newDlogConf(flags);
        try (Namespace namespace = NamespaceBuilder.newBuilder()
             .conf(conf)
             .uri(serviceURI.getUri())
             .build()) {
            execute(namespace);
        }
    }

    protected void reportStats() {
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

            log.info()
                    .attr("rate", THROUGHPUT_FORMAT.format(rate))
                    .attr("throughputMBs", THROUGHPUT_FORMAT.format(throughput))
                    .attr("latencyMeanMs", PADDING_DECIMAL_FORMAT.format(reportHistogram.getMean() / 1000.0))
                    .attr("latencyMedMs",
                        PADDING_DECIMAL_FORMAT.format(reportHistogram.getValueAtPercentile(50) / 1000.0))
                    .attr("latency95pctMs",
                        PADDING_DECIMAL_FORMAT.format(reportHistogram.getValueAtPercentile(95) / 1000.0))
                    .attr("latency99pctMs",
                        PADDING_DECIMAL_FORMAT.format(reportHistogram.getValueAtPercentile(99) / 1000.0))
                    .attr("latency999pctMs",
                        PADDING_DECIMAL_FORMAT.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0))
                    .attr("latency9999pctMs",
                        PADDING_DECIMAL_FORMAT.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0))
                    .attr("latencyMaxMs", PADDING_DECIMAL_FORMAT.format(reportHistogram.getMaxValue() / 1000.0))
                    .log("Throughput read");

            reportHistogram.reset();

            oldTime = now;
        }

    }

    protected abstract void execute(Namespace namespace) throws Exception;

    @Override
    public void run() {
        try {
            execute();
        } catch (Exception e) {
            log.error().exception(e).log("Encountered exception at running dlog perf writer");
        }
    }

    private static DistributedLogConfiguration newDlogConf(Flags flags) {
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
            .setReadAheadBatchSize(flags.readAheadBatchSize)
            .setReadAheadMaxRecords(flags.maxReadAheadRecords)
            .setReadAheadWaitTime(200);
        conf.setProperty("bkc.numChannelsPerBookie", 8);
        return conf;
    }

    protected static final DecimalFormat THROUGHPUT_FORMAT = new PaddingDecimalFormat("0.0", 8);
    protected static final DecimalFormat PADDING_DECIMAL_FORMAT = new PaddingDecimalFormat("0.000", 7);

    protected static void printAggregatedStats(Recorder recorder) {
        Histogram reportHistogram = recorder.getIntervalHistogram();

        log.info()
                .attr("latencyMeanMs", PADDING_DECIMAL_FORMAT.format(reportHistogram.getMean() / 1000.0))
                .attr("latencyMedMs",
                    PADDING_DECIMAL_FORMAT.format(reportHistogram.getValueAtPercentile(50) / 1000.0))
                .attr("latency95pctMs",
                    PADDING_DECIMAL_FORMAT.format(reportHistogram.getValueAtPercentile(95) / 1000.0))
                .attr("latency99pctMs",
                    PADDING_DECIMAL_FORMAT.format(reportHistogram.getValueAtPercentile(99) / 1000.0))
                .attr("latency999pctMs",
                    PADDING_DECIMAL_FORMAT.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0))
                .attr("latency9999pctMs",
                    PADDING_DECIMAL_FORMAT.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0))
                .attr("latency99999pctMs",
                    PADDING_DECIMAL_FORMAT.format(reportHistogram.getValueAtPercentile(99.999) / 1000.0))
                .attr("latencyMaxMs", PADDING_DECIMAL_FORMAT.format(reportHistogram.getMaxValue() / 1000.0))
                .log("Aggregated latency stats");
    }

}
