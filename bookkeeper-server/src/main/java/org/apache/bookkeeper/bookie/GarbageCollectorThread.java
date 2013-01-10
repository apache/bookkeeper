/**
 *
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
 *
 */

package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.GarbageCollector.GarbageCleaner;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SnapshotMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the garbage collector thread that runs in the background to
 * remove any entry log files that no longer contains any active ledger.
 */
public class GarbageCollectorThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(GarbageCollectorThread.class);
    private static final int COMPACTION_MAX_OUTSTANDING_REQUESTS = 1000;
    private static final int SECOND = 1000;

    // Maps entry log files to the set of ledgers that comprise the file and the size usage per ledger
    private Map<Long, EntryLogMetadata> entryLogMetaMap = new ConcurrentHashMap<Long, EntryLogMetadata>();

    // This is how often we want to run the Garbage Collector Thread (in milliseconds).
    final long gcWaitTime;

    // Compaction parameters
    boolean enableMinorCompaction = false;
    final double minorCompactionThreshold;
    final long minorCompactionInterval;

    boolean enableMajorCompaction = false;
    final double majorCompactionThreshold;
    final long majorCompactionInterval;

    long lastMinorCompactionTime;
    long lastMajorCompactionTime;

    // Entry Logger Handle
    final EntryLogger entryLogger;
    final SafeEntryAdder safeEntryAdder;

    // Ledger Cache Handle
    final LedgerCache ledgerCache;
    final SnapshotMap<Long, Boolean> activeLedgers;

    // flag to ensure gc thread will not be interrupted during compaction
    // to reduce the risk getting entry log corrupted
    final AtomicBoolean compacting = new AtomicBoolean(false);

    volatile boolean running = true;

    // track the last scanned successfully log id
    long scannedLogId = 0;

    final GarbageCollector garbageCollector;
    final GarbageCleaner garbageCleaner;


    /**
     * Interface for adding entries. When the write callback is triggered, the
     * entry must be guaranteed to be presisted.
     */
    interface SafeEntryAdder {
        public void safeAddEntry(long ledgerId, ByteBuffer buffer, GenericCallback<Void> cb);
    }

    /**
     * A scanner wrapper to check whether a ledger is alive in an entry log file
     */
    class CompactionScanner implements EntryLogScanner {
        EntryLogMetadata meta;
        Object completionLock = new Object();
        AtomicInteger outstandingRequests = new AtomicInteger(0);
        AtomicBoolean allSuccessful = new AtomicBoolean(true);

        public CompactionScanner(EntryLogMetadata meta) {
            this.meta = meta;
        }

        @Override
        public boolean accept(long ledgerId) {
            return meta.containsLedger(ledgerId);
        }

        @Override
        public void process(final long ledgerId, long offset, ByteBuffer entry)
            throws IOException {
            if (!allSuccessful.get()) {
                return;
            }

            outstandingRequests.incrementAndGet();
            synchronized (completionLock) {
                while (outstandingRequests.get() >= COMPACTION_MAX_OUTSTANDING_REQUESTS) {
                    try {
                        completionLock.wait();
                    } catch (InterruptedException ie) {
                        LOG.error("Interrupted while waiting to re-add entry", ie);
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting to re-add entry", ie);
                    }
                }
            }
            safeEntryAdder.safeAddEntry(ledgerId, entry, new GenericCallback<Void>() {
                    @Override
                    public void operationComplete(int rc, Void result) {
                        if (rc != BookieException.Code.OK) {
                            LOG.error("Error {} re-adding entry for ledger {})",
                                      rc, ledgerId);
                            allSuccessful.set(false);
                        }
                        synchronized(completionLock) {
                            outstandingRequests.decrementAndGet();
                            completionLock.notifyAll();
                        }
                    }
                });
        }

        void awaitComplete() throws InterruptedException, IOException {
            synchronized(completionLock) {
                while (outstandingRequests.get() > 0) {
                    completionLock.wait();
                }
                if (allSuccessful.get() == false) {
                    throw new IOException("Couldn't re-add all entries");
                }
            }
        }
    }


    /**
     * Create a garbage collector thread.
     *
     * @param conf
     *          Server Configuration Object.
     * @throws IOException
     */
    public GarbageCollectorThread(ServerConfiguration conf,
                                  final LedgerCache ledgerCache,
                                  EntryLogger entryLogger,
                                  SnapshotMap<Long, Boolean> activeLedgers,
                                  SafeEntryAdder safeEntryAdder,
                                  LedgerManager ledgerManager)
        throws IOException {
        super("GarbageCollectorThread");

        this.ledgerCache = ledgerCache;
        this.entryLogger = entryLogger;
        this.activeLedgers = activeLedgers;
        this.safeEntryAdder = safeEntryAdder;

        this.gcWaitTime = conf.getGcWaitTime();

        this.garbageCleaner = new GarbageCollector.GarbageCleaner() {
            @Override
            public void clean(long ledgerId) {
                try {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("delete ledger : " + ledgerId);
                    }
                    ledgerCache.deleteLedger(ledgerId);
                } catch (IOException e) {
                    LOG.error("Exception when deleting the ledger index file on the Bookie: ", e);
                }
            }
        };

        this.garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, activeLedgers);

        // compaction parameters
        minorCompactionThreshold = conf.getMinorCompactionThreshold();
        minorCompactionInterval = conf.getMinorCompactionInterval() * SECOND;
        majorCompactionThreshold = conf.getMajorCompactionThreshold();
        majorCompactionInterval = conf.getMajorCompactionInterval() * SECOND;

        if (minorCompactionInterval > 0 && minorCompactionThreshold > 0) {
            if (minorCompactionThreshold > 1.0f) {
                throw new IOException("Invalid minor compaction threshold "
                                    + minorCompactionThreshold);
            }
            if (minorCompactionInterval <= gcWaitTime) {
                throw new IOException("Too short minor compaction interval : "
                                    + minorCompactionInterval);
            }
            enableMinorCompaction = true;
        }

        if (majorCompactionInterval > 0 && majorCompactionThreshold > 0) {
            if (majorCompactionThreshold > 1.0f) {
                throw new IOException("Invalid major compaction threshold "
                                    + majorCompactionThreshold);
            }
            if (majorCompactionInterval <= gcWaitTime) {
                throw new IOException("Too short major compaction interval : "
                                    + majorCompactionInterval);
            }
            enableMajorCompaction = true;
        }

        if (enableMinorCompaction && enableMajorCompaction) {
            if (minorCompactionInterval >= majorCompactionInterval ||
                minorCompactionThreshold >= majorCompactionThreshold) {
                throw new IOException("Invalid minor/major compaction settings : minor ("
                                    + minorCompactionThreshold + ", " + minorCompactionInterval
                                    + "), major (" + majorCompactionThreshold + ", "
                                    + majorCompactionInterval + ")");
            }
        }

        LOG.info("Minor Compaction : enabled=" + enableMinorCompaction + ", threshold="
               + minorCompactionThreshold + ", interval=" + minorCompactionInterval);
        LOG.info("Major Compaction : enabled=" + enableMajorCompaction + ", threshold="
               + majorCompactionThreshold + ", interval=" + majorCompactionInterval);

        lastMinorCompactionTime = lastMajorCompactionTime = MathUtils.now();
    }

    @Override
    public void run() {
        while (running) {
            synchronized (this) {
                try {
                    wait(gcWaitTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                }
            }

            // Extract all of the ledger ID's that comprise all of the entry logs
            // (except for the current new one which is still being written to).
            entryLogMetaMap = extractMetaFromEntryLogs(entryLogMetaMap);

            // gc inactive/deleted ledgers
            doGcLedgers();

            // gc entry logs
            doGcEntryLogs();

            long curTime = MathUtils.now();
            if (enableMajorCompaction &&
                curTime - lastMajorCompactionTime > majorCompactionInterval) {
                // enter major compaction
                LOG.info("Enter major compaction");
                doCompactEntryLogs(majorCompactionThreshold);
                lastMajorCompactionTime = MathUtils.now();
                // also move minor compaction time
                lastMinorCompactionTime = lastMajorCompactionTime;
                continue;
            }

            if (enableMinorCompaction &&
                curTime - lastMinorCompactionTime > minorCompactionInterval) {
                // enter minor compaction
                LOG.info("Enter minor compaction");
                doCompactEntryLogs(minorCompactionThreshold);
                lastMinorCompactionTime = MathUtils.now();
            }
        }
    }

    /**
     * Do garbage collection ledger index files
     */
    private void doGcLedgers() {
        garbageCollector.gc(garbageCleaner);
    }

    /**
     * Garbage collect those entry loggers which are not associated with any active ledgers
     */
    private void doGcEntryLogs() {
        // Loop through all of the entry logs and remove the non-active ledgers.
        for (Long entryLogId : entryLogMetaMap.keySet()) {
            EntryLogMetadata meta = entryLogMetaMap.get(entryLogId);
            for (Long entryLogLedger : meta.ledgersMap.keySet()) {
                // Remove the entry log ledger from the set if it isn't active.
                if (!activeLedgers.containsKey(entryLogLedger)) {
                    meta.removeLedger(entryLogLedger);
                }
            }
            if (meta.isEmpty()) {
                // This means the entry log is not associated with any active ledgers anymore.
                // We can remove this entry log file now.
                LOG.info("Deleting entryLogId " + entryLogId + " as it has no active ledgers!");
                removeEntryLog(entryLogId);
            }
        }
    }

    /**
     * Compact entry logs if necessary.
     *
     * <p>
     * Compaction will be executed from low unused space to high unused space.
     * Those entry log files whose remaining size percentage is higher than threshold
     * would not be compacted.
     * </p>
     */
    private void doCompactEntryLogs(double threshold) {
        LOG.info("Do compaction to compact those files lower than " + threshold);
        // sort the ledger meta by occupied unused space
        Comparator<EntryLogMetadata> sizeComparator = new Comparator<EntryLogMetadata>() {
            @Override
            public int compare(EntryLogMetadata m1, EntryLogMetadata m2) {
                long unusedSize1 = m1.totalSize - m1.remainingSize;
                long unusedSize2 = m2.totalSize - m2.remainingSize;
                if (unusedSize1 > unusedSize2) {
                    return -1;
                } else if (unusedSize1 < unusedSize2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };
        List<EntryLogMetadata> logsToCompact = new ArrayList<EntryLogMetadata>();
        logsToCompact.addAll(entryLogMetaMap.values());
        Collections.sort(logsToCompact, sizeComparator);
        for (EntryLogMetadata meta : logsToCompact) {
            if (meta.getUsage() >= threshold) {
                break;
            }
            LOG.debug("Compacting entry log {} below threshold {}.", meta.entryLogId, threshold);
            compactEntryLog(meta.entryLogId);
            if (!running) { // if gc thread is not running, stop compaction
                return;
            }
        }
    }

    /**
     * Shutdown the garbage collector thread.
     *
     * @throws InterruptedException if there is an exception stopping gc thread.
     */
    public void shutdown() throws InterruptedException {
        this.running = false;
        if (compacting.compareAndSet(false, true)) {
            // if setting compacting flag succeed, means gcThread is not compacting now
            // it is safe to interrupt itself now
            this.interrupt();
        }
        this.join();
    }

    /**
     * Remove entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    private void removeEntryLog(long entryLogId) {
        // remove entry log file successfully
        if (entryLogger.removeEntryLog(entryLogId)) {
            entryLogMetaMap.remove(entryLogId);
        }
    }

    /**
     * Compact an entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    protected void compactEntryLog(long entryLogId) {
        EntryLogMetadata entryLogMeta = entryLogMetaMap.get(entryLogId);
        if (null == entryLogMeta) {
            LOG.warn("Can't get entry log meta when compacting entry log " + entryLogId + ".");
            return;
        }

        // Similar with Sync Thread
        // try to mark compacting flag to make sure it would not be interrupted
        // by shutdown during compaction. otherwise it will receive
        // ClosedByInterruptException which may cause index file & entry logger
        // closed and corrupted.
        if (!compacting.compareAndSet(false, true)) {
            // set compacting flag failed, means compacting is true now
            // indicates another thread wants to interrupt gc thread to exit
            return;
        }

        LOG.info("Compacting entry log : " + entryLogId);

        try {
            CompactionScanner scanner = new CompactionScanner(entryLogMeta);
            entryLogger.scanEntryLog(entryLogId, scanner);
            scanner.awaitComplete();
            // after moving entries to new entry log, remove this old one
            removeEntryLog(entryLogId);
        } catch (IOException e) {
            LOG.info("Premature exception when compacting " + entryLogId, e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while compacting", ie);
        } finally {
            // clear compacting flag
            compacting.set(false);
        }
    }

    /**
     * Records the total size, remaining size and the set of ledgers that comprise a entry log.
     */
    static class EntryLogMetadata {
        long entryLogId;
        long totalSize;
        long remainingSize;
        ConcurrentHashMap<Long, Long> ledgersMap;

        public EntryLogMetadata(long logId) {
            this.entryLogId = logId;

            totalSize = remainingSize = 0;
            ledgersMap = new ConcurrentHashMap<Long, Long>();
        }

        public void addLedgerSize(long ledgerId, long size) {
            totalSize += size;
            remainingSize += size;
            Long ledgerSize = ledgersMap.get(ledgerId);
            if (null == ledgerSize) {
                ledgerSize = 0L;
            }
            ledgerSize += size;
            ledgersMap.put(ledgerId, ledgerSize);
        }

        public void removeLedger(long ledgerId) {
            Long size = ledgersMap.remove(ledgerId);
            if (null == size) {
                return;
            }
            remainingSize -= size;
        }

        public boolean containsLedger(long ledgerId) {
            return ledgersMap.containsKey(ledgerId);
        }

        public double getUsage() {
            if (totalSize == 0L) {
                return 0.0f;
            }
            return (double)remainingSize / totalSize;
        }

        public boolean isEmpty() {
            return ledgersMap.isEmpty();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{ totalSize = ").append(totalSize).append(", remainingSize = ")
              .append(remainingSize).append(", ledgersMap = ").append(ledgersMap).append(" }");
            return sb.toString();
        }
    }

    /**
     * A scanner used to extract entry log meta from entry log files.
     */
    static class ExtractionScanner implements EntryLogScanner {
        EntryLogMetadata meta;

        public ExtractionScanner(EntryLogMetadata meta) {
            this.meta = meta;
        }

        @Override
        public boolean accept(long ledgerId) {
            return true;
        }
        @Override
        public void process(long ledgerId, long offset, ByteBuffer entry) {
            // add new entry size of a ledger to entry log meta
            meta.addLedgerSize(ledgerId, entry.limit() + 4);
        }
    }

    /**
     * Method to read in all of the entry logs (those that we haven't done so yet),
     * and find the set of ledger ID's that make up each entry log file.
     *
     * @param entryLogMetaMap
     *          Existing EntryLogs to Meta
     * @throws IOException
     */
    protected Map<Long, EntryLogMetadata> extractMetaFromEntryLogs(Map<Long, EntryLogMetadata> entryLogMetaMap) {
        // Extract it for every entry log except for the current one.
        // Entry Log ID's are just a long value that starts at 0 and increments
        // by 1 when the log fills up and we roll to a new one.
        long curLogId = entryLogger.getCurrentLogId();
        boolean hasExceptionWhenScan = false;
        for (long entryLogId = scannedLogId; entryLogId < curLogId; entryLogId++) {
            // Comb the current entry log file if it has not already been extracted.
            if (entryLogMetaMap.containsKey(entryLogId)) {
                continue;
            }

            // check whether log file exists or not
            // if it doesn't exist, this log file might have been garbage collected.
            if (!entryLogger.logExists(entryLogId)) {
                continue;
            }

            LOG.info("Extracting entry log meta from entryLogId: " + entryLogId);

            try {
                // Read through the entry log file and extract the entry log meta
                EntryLogMetadata entryLogMeta = extractMetaFromEntryLog(entryLogger, entryLogId);
                entryLogMetaMap.put(entryLogId, entryLogMeta);
            } catch (IOException e) {
                hasExceptionWhenScan = true;
                LOG.warn("Premature exception when processing " + entryLogId +
                         " recovery will take care of the problem", e);
            }

            // if scan failed on some entry log, we don't move 'scannedLogId' to next id
            // if scan succeed, we don't need to scan it again during next gc run,
            // we move 'scannedLogId' to next id
            if (!hasExceptionWhenScan) {
                ++scannedLogId;
            }
        }
        return entryLogMetaMap;
    }

    static EntryLogMetadata extractMetaFromEntryLog(EntryLogger entryLogger, long entryLogId)
            throws IOException {
        EntryLogMetadata entryLogMeta = new EntryLogMetadata(entryLogId);
        ExtractionScanner scanner = new ExtractionScanner(entryLogMeta);
        // Read through the entry log file and extract the entry log meta
        entryLogger.scanEntryLog(entryLogId, scanner);
        LOG.info("Retrieved entry log meta data entryLogId: "
                 + entryLogId + ", meta: " + entryLogMeta);
        return entryLogMeta;
    }
}
