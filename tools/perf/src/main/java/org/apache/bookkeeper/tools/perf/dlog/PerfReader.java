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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.namespace.Namespace;

/**
 * A perf reader to evaluate read performance.
 */
@CustomLog
public class PerfReader extends PerfReaderBase {

    PerfReader(ServiceURI serviceURI, Flags flags) {
        super(serviceURI, flags);
    }

    @Override
    protected void execute(Namespace namespace) throws Exception {
        List<Pair<Integer, DistributedLogManager>> managers = new ArrayList<>(flags.numLogs);
        for (int i = 0; i < flags.numLogs; i++) {
            String logName = String.format(flags.logName, i);
            managers.add(Pair.of(i, namespace.openLog(logName)));
        }
        log.info().attr("numLogs", managers.size()).log("Successfully opened logs");

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
                        log.error().exception(e).log("Encountered error at writing records");
                    }
                });
            }
            log.info().attr("numThreads", flags.numThreads).log("Started write threads");
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
        log.info()
                .attr("logs", logs.stream().map(l -> l.getStreamName()).collect(Collectors.toList()))
                .log("Read thread started");

        List<LogReader> readers = logs.stream()
            .map(manager -> {
                try {
                    return manager.openLogReader(DLSN.InitialDLSN);
                } catch (IOException e) {
                    log.error()
                            .attr("streamName", manager.getStreamName())
                            .exception(e)
                            .log("Failed to open reader for log stream");
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





}
