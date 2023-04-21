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

package org.apache.bookkeeper.tools.perf.journal;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.JOURNAL_SCOPE;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.util.concurrent.RateLimiter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Journal;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.Stats;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.perf.utils.PaddingDecimalFormat;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.configuration.CompositeConfiguration;

/**
 * A perf writer to evaluate write performance.
 */
@Slf4j
public class JournalWriter implements Runnable {

    /**
     * Flags for the write command.
     */
    public static class Flags extends CliFlags {

        @Parameter(
            names = {
                "-t", "--num-test-threads"
            },
            description = "Num of test threads to append entries to journal"
        )
        public int numTestThreads = 1;

        @Parameter(
            names = {
                "-nl", "--num-ledgers"
            },
            description = "Num of ledgers to append entries to journal"
        )
        public int numLedgers = 24;

        @Parameter(
            names = {
                "-r", "--rate"
            },
            description = "Write rate bytes/s across journals")
        public long writeRate = 0;

        @Parameter(
            names = {
                "-s", "--entry-size"
            },
            description = "Entry size")
        public int recordSize = 1024;

        @Parameter(
            names = {
                "-j", "--journal-dirs"
            },
            description = "The list of journal directories, separated by comma",
            required = true)
        public List<String> journalDirs;

        @Parameter(
            names = {
                "-mob", "--max-outstanding-megabytes"
            },
            description = "Number of threads writing")
        public long maxOutstandingMB = 200;

        @Parameter(
            names = {
                "-n", "--num-entries"
            },
            description = "Number of entries to write in total. If 0, it will keep writing")
        public long numEntries = 0;

        @Parameter(
            names = {
                "-b", "--num-bytes"
            },
            description = "Number of bytes to write in total. If 0, it will keep writing")
        public long numBytes = 0;

        @Parameter(
            names = {
                "-wb", "--write-buffer-size-kb"
            },
            description = "Journal write buffer size"
        )
        public int writeBufferSizeKB = 1024;

        @Parameter(
            names = {
                "--sync"
            },
            description = "Journal sync enabled"
        )
        public boolean journalSyncEnabled = false;

        @Parameter(
            names = {
                "-gci", "--group-commit-interval-ms"
            },
            description = "Journal group commit interval in milliseconds"
        )
        public int groupCommitIntervalMs = 1;

        @Parameter(
            names = {
                "-gcb", "--group-commit-max-bytes"
            },
            description = "Journal group commit max buffered bytes"
        )
        public int groupCommitMaxBytes = 512 * 1024;

        @Parameter(
            names = {
                "-q", "--journal-queue-size"
            },
            description = "Journal queue size"
        )
        public int journalQueueSize = 10000;

        @Parameter(
            names = {
                "-jt", "--num-journal-callback-threads"
            },
            description = "Number of journal callback threads"
        )
        public int numJournalCallbackThreads = 8;

    }


    // stats
    private final LongAdder recordsWritten = new LongAdder();
    private final LongAdder bytesWritten = new LongAdder();

    private final ServerConfiguration conf;
    private final Flags flags;
    private final Recorder recorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );
    private final Recorder cumulativeRecorder = new Recorder(
        TimeUnit.SECONDS.toMillis(120000), 5
    );
    private final AtomicBoolean isDone = new AtomicBoolean(false);

    JournalWriter(CompositeConfiguration conf, Flags flags) {
        this.conf = new ServerConfiguration();
        this.conf.addConfiguration(conf);
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
        log.info("Starting journal perf writer with config : {}", w.writeValueAsString(flags));

        checkArgument(flags.journalDirs.size() > 0, "No journal dirs is provided");

        updateServerConf(conf, flags);

        log.info("Benchmark the journal perf with server config : {}",
            conf.asJson());

        Stats.loadStatsProvider(conf);
        Stats.get().start(conf);

        StatsLogger statsLogger = Stats.get().getStatsLogger("")
            .scope(BOOKIE_SCOPE);


        ByteBufAllocator allocator = getAllocator(conf);
        DiskChecker checker = new DiskChecker(
            conf.getDiskUsageThreshold(),
            conf.getDiskUsageWarnThreshold()
        );
        LedgerDirsManager manager = new LedgerDirsManager(
            conf,
            conf.getLedgerDirs(),
            checker,
            NullStatsLogger.INSTANCE
        );
        Journal[] journals = new Journal[flags.journalDirs.size()];
        for (int i = 0; i < journals.length; i++) {
            Journal journal = new Journal(
                i,
                new File(flags.journalDirs.get(i)),
                conf,
                manager,
                statsLogger.scope(JOURNAL_SCOPE),
                allocator);
            journals[i] = journal;
            journal.start();
        }
        try {
            execute(journals);
        } finally {
            for (Journal journal : journals) {
                journal.shutdown();
            }

            Stats.get().stop();
        }
    }

    void execute(Journal[] journals) throws Exception {
        // register shutdown hook to aggregate stats
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isDone.set(true);
            printAggregatedStats(cumulativeRecorder);
        }));

        ScheduledExecutorService flushExecutor = Executors.newSingleThreadScheduledExecutor();
        flushExecutor.scheduleAtFixedRate(() -> {
            for (Journal journal : journals) {
                Checkpoint cp = journal.newCheckpoint();
                try {
                    journal.checkpointComplete(cp, true);
                } catch (IOException e) {
                    log.error("Failed to complete checkpoint {}", cp, e);
                }
            }
        }, 30L, 30L, TimeUnit.SECONDS);

        ExecutorService executor = Executors.newFixedThreadPool(flags.numTestThreads);
        try {
            for (int i = 0; i < flags.numTestThreads; i++) {
                final int idx = i;
                final long numRecordsForThisThread = flags.numEntries / flags.numTestThreads;
                final long numBytesForThisThread = flags.numBytes / flags.numTestThreads;
                final double writeRateForThisThread = flags.writeRate / (double) flags.numTestThreads;
                final long maxOutstandingBytesForThisThread =
                    flags.maxOutstandingMB * 1024 * 1024 / flags.numTestThreads;
                final int numLedgersForThisThread = flags.numLedgers / flags.numTestThreads;
                executor.submit(() -> {
                    try {
                        write(
                            idx,
                            journals,
                            numLedgersForThisThread,
                            writeRateForThisThread,
                            (int) maxOutstandingBytesForThisThread,
                            numRecordsForThisThread,
                            numBytesForThisThread);
                    } catch (Throwable t) {
                        log.error("Encountered error at writing records", t);
                    }
                });
            }
            log.info("Started {} write threads", flags.numTestThreads);
            reportStats();
        } finally {
            flushExecutor.shutdown();
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    void write(int threadIdx,
               Journal[] journals,
               int numLedgersForThisThread,
               double writeRate,
               int maxOutstandingBytesForThisThread,
               long numRecordsForThisThread,
               long numBytesForThisThread) throws Exception {
        log.info("Write thread {} started with : rate = {},"
                + " num records = {}, num bytes = {}, max outstanding bytes = {}",
            threadIdx,
            writeRate,
            numRecordsForThisThread,
            numBytesForThisThread,
            maxOutstandingBytesForThisThread);

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
        final int numJournals = journals.length;
        byte[] payload = new byte[flags.recordSize];
        ThreadLocalRandom.current().nextBytes(payload);
        ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
        long[] entryIds = new long[numLedgersForThisThread];
        Arrays.fill(entryIds, 0L);
        while (true) {
            for (int i = 0; i < numJournals; i++) {
                int ledgerIdx = ThreadLocalRandom.current().nextInt(numLedgersForThisThread);
                long lid = threadIdx * numLedgersForThisThread + ledgerIdx;
                long eid = entryIds[ledgerIdx]++;
                ByteBuf buf = payloadBuf.retainedDuplicate();
                int len = buf.readableBytes();

                if (numRecordsForThisThread > 0
                    && totalWritten >= numRecordsForThisThread) {
                    markPerfDone();
                }
                if (numBytesForThisThread > 0
                    && totalBytesWritten >= numBytesForThisThread) {
                    markPerfDone();
                }
                if (null != semaphore) {
                    semaphore.acquire(len);
                }

                totalWritten++;
                totalBytesWritten += len;
                if (null != limiter) {
                    limiter.acquire(len);
                }
                final long sendTime = System.nanoTime();
                journals[i].logAddEntry(
                    lid,
                    eid,
                    buf,
                    false,
                    (rc, ledgerId, entryId, addr, ctx) -> {
                        ReferenceCountUtil.release(buf);
                        if (0 == rc) {
                            if (null != semaphore) {
                                semaphore.release(len);
                            }

                            recordsWritten.increment();
                            bytesWritten.add(len);

                            long latencyMicros = TimeUnit.NANOSECONDS.toMicros(
                                System.nanoTime() - sendTime
                            );
                            recorder.recordValue(latencyMicros);
                            cumulativeRecorder.recordValue(latencyMicros);
                        } else {
                            log.warn("Error at writing records : ", BookieException.create(rc));
                            Runtime.getRuntime().exit(-1);
                        }
                    },
                    null
                );
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

    private static void updateServerConf(ServerConfiguration conf, Flags flags) {

        conf.setJournalWriteBufferSizeKB(flags.writeBufferSizeKB);
        conf.setJournalMaxGroupWaitMSec(flags.groupCommitIntervalMs);
        conf.setJournalBufferedWritesThreshold(flags.groupCommitMaxBytes);
        conf.setNumJournalCallbackThreads(flags.numJournalCallbackThreads);
        conf.setJournalQueueSize(flags.journalQueueSize);
        conf.setJournalSyncData(flags.journalSyncEnabled);
        conf.setLedgerDirNames(flags.journalDirs.toArray(new String[0]));
        conf.setStatsProviderClass(PrometheusMetricsProvider.class);
        File[] currentDirs = BookieImpl.getCurrentDirectories(conf.getLedgerDirs());
        for (File dir : currentDirs) {
            if (dir.mkdirs()) {
                log.info("Successfully created dir {}", dir);
            }
        }
    }

    private static ByteBufAllocator getAllocator(ServerConfiguration conf) {
        return ByteBufAllocatorBuilder.create()
                .poolingPolicy(conf.getAllocatorPoolingPolicy())
                .poolingConcurrency(conf.getAllocatorPoolingConcurrency())
                .outOfMemoryPolicy(conf.getAllocatorOutOfMemoryPolicy())
                .outOfMemoryListener((ex) -> {
                    log.error("Unable to allocate memory, exiting bookie", ex);
                })
                .leakDetectionPolicy(conf.getAllocatorLeakDetectionPolicy())
                .build();
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
