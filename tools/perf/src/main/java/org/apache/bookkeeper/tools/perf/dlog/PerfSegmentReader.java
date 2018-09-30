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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.Entry;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.exceptions.EndOfLogSegmentException;
import org.apache.distributedlog.logsegment.LogSegmentEntryReader;
import org.apache.distributedlog.namespace.NamespaceDriver.Role;

/**
 * A perf writer to evaluate write performance.
 */
@Slf4j
public class PerfSegmentReader extends PerfReaderBase {

    @Data
    static class Split {
        final DistributedLogManager manager;
        final LogSegmentMetadata segment;
        final long startEntryId;
        final long endEntryId;
    }

    PerfSegmentReader(ServiceURI serviceURI, Flags flags) {
        super(serviceURI, flags);
    }

    @Override
    protected void execute(Namespace namespace) throws Exception {
        List<DistributedLogManager> managers = new ArrayList<>(flags.numLogs);
        for (int i = 0; i < flags.numLogs; i++) {
            String logName = String.format(flags.logName, i);
            managers.add(namespace.openLog(logName));
        }
        log.info("Successfully open {} logs", managers.size());

        // Get all the log segments
        final List<Pair<DistributedLogManager, LogSegmentMetadata>> segments = managers.stream()
            .flatMap(manager -> {
                try {
                    return manager.getLogSegments().stream().map(segment -> Pair.of(manager, segment));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            })
            .collect(Collectors.toList());

        final List<Split> splits = segments.stream()
            .flatMap(entry -> getNumSplits(entry.getLeft(), entry.getRight()).stream())
            .collect(Collectors.toList());

        // register shutdown hook to aggregate stats
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            isDone.set(true);
            printAggregatedStats(cumulativeRecorder);
        }));

        ExecutorService executor = Executors.newFixedThreadPool(flags.numThreads);
        try {
            for (int i = 0; i < flags.numThreads; i++) {
                final int idx = i;
                final List<Split> splitsThisThread = splits
                    .stream()
                    .filter(split -> splits.indexOf(split) % flags.numThreads == idx)
                    .collect(Collectors.toList());
                executor.submit(() -> {
                    try {
                        read(splitsThisThread);
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
            managers.forEach(manager -> manager.asyncClose());
        }
    }

    void read(List<Split> splits) throws Exception {
        log.info("Read thread started with : splits = {}",
            splits.stream()
                .map(l -> "(log = " + l.manager.getStreamName() + ", segment = "
                    + l.segment.getLogSegmentSequenceNumber() + " ["  + l.startEntryId + ", " + l.endEntryId + "])")
                .collect(Collectors.toList()));

        splits.forEach(entry -> {
            try {
                readSegmentSplit(entry);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    void readSegmentSplit(Split split) throws Exception {
        LogSegmentEntryReader reader = result(split.manager.getNamespaceDriver().getLogSegmentEntryStore(Role.READER)
            .openReader(split.segment, split.getStartEntryId()));
        reader.start();

        try {
            MutableBoolean isDone = new MutableBoolean(false);
            while (!isDone.booleanValue()) {
                // 100 is just an indicator
                List<Entry.Reader> entries = result(reader.readNext(100));
                entries.forEach(entry -> {
                    LogRecordWithDLSN record;
                    try {
                        while ((record = entry.nextRecord()) != null) {
                            recordsRead.increment();
                            bytesRead.add(record.getPayloadBuf().readableBytes());
                        }
                    } catch (IOException ioe) {
                        throw new UncheckedIOException(ioe);
                    } finally {
                        entry.release();
                    }
                    if (split.getEndEntryId() >= 0 && entry.getEntryId() >= split.getEndEntryId()) {
                        isDone.setValue(true);
                    }
                });
            }
        } catch (EndOfLogSegmentException e) {
            // we reached end of log segment
            return;
        } finally {
            reader.asyncClose();
        }

    }

    List<Split> getNumSplits(DistributedLogManager manager, LogSegmentMetadata segment) {
        if (flags.numSplitsPerSegment <= 1) {
            // do split
            return Lists.newArrayList(
                new Split(
                    manager,
                    segment,
                    0L,
                    -1L)
            );
        } else {
            long lastEntryId = segment.getLastEntryId();
            long numEntriesPerSplit = (lastEntryId + 1) / 2;
            long nextEntryId = 0L;
            List<Split> splitsInSegment = new ArrayList<>(flags.numSplitsPerSegment);
            for (int i = 0; i < flags.numSplitsPerSegment; i++) {
                long startEntryId = nextEntryId;
                long endEntryId;
                if (i == flags.numSplitsPerSegment - 1) {
                    endEntryId = lastEntryId;
                } else {
                    endEntryId = nextEntryId + numEntriesPerSplit - 1;
                }
                splitsInSegment.add(new Split(
                    manager,
                    segment,
                    startEntryId,
                    endEntryId
                ));
                nextEntryId = endEntryId + 1;
            }
            return splitsInSegment;
        }
    }

}
