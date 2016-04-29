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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.GarbageCollector.GarbageCleaner;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the garbage collector thread that runs in the background to
 * remove any entry log files that no longer contains any active ledger.
 */
public class GarbageCollectorThread extends BookieThread {
    private static final Logger LOG = LoggerFactory.getLogger(GarbageCollectorThread.class);
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

    final boolean isForceGCAllowWhenNoSpace;

    long lastMinorCompactionTime;
    long lastMajorCompactionTime;

    final boolean isThrottleByBytes;
    final int maxOutstandingRequests;
    final int compactionRateByEntries;
    final int compactionRateByBytes;
    final CompactionScannerFactory scannerFactory;

    // Entry Logger Handle
    final EntryLogger entryLogger;

    final CompactableLedgerStorage ledgerStorage;

    // flag to ensure gc thread will not be interrupted during compaction
    // to reduce the risk getting entry log corrupted
    final AtomicBoolean compacting = new AtomicBoolean(false);

    volatile boolean running = true;

    // track the last scanned successfully log id
    long scannedLogId = 0;

    // Boolean to trigger a forced GC.
    final AtomicBoolean forceGarbageCollection = new AtomicBoolean(false);
    // Boolean to disable major compaction, when disk is almost full
    final AtomicBoolean suspendMajorCompaction = new AtomicBoolean(false);
    // Boolean to disable minor compaction, when disk is full
    final AtomicBoolean suspendMinorCompaction = new AtomicBoolean(false);

    final GarbageCollector garbageCollector;
    final GarbageCleaner garbageCleaner;

    private static class Throttler {
        final RateLimiter rateLimiter;
        final boolean isThrottleByBytes;
        final int compactionRateByBytes;
        final int compactionRateByEntries;

        Throttler(boolean isThrottleByBytes,
                  int compactionRateByBytes,
                  int compactionRateByEntries) {
            this.isThrottleByBytes  = isThrottleByBytes;
            this.compactionRateByBytes = compactionRateByBytes;
            this.compactionRateByEntries = compactionRateByEntries;
            this.rateLimiter = RateLimiter.create(this.isThrottleByBytes ?
                                                  this.compactionRateByBytes :
                                                  this.compactionRateByEntries);
        }

        // acquire. if bybytes: bytes of this entry; if byentries: 1.
        void acquire(int permits) {
            rateLimiter.acquire(this.isThrottleByBytes ? permits : 1);
        }
    }

    /**
     * A scanner wrapper to check whether a ledger is alive in an entry log file
     */
    class CompactionScannerFactory {
        List<EntryLocation> offsets = new ArrayList<EntryLocation>();

        EntryLogScanner newScanner(final EntryLogMetadata meta) {
            final Throttler throttler = new Throttler (isThrottleByBytes,
                                                       compactionRateByBytes,
                                                       compactionRateByEntries);

            return new EntryLogScanner() {
                @Override
                public boolean accept(long ledgerId) {
                    return meta.containsLedger(ledgerId);
                }

                @Override
                public void process(final long ledgerId, long offset, ByteBuffer entry) throws IOException {
                    throttler.acquire(entry.remaining());

                    if (offsets.size() > maxOutstandingRequests) {
                        flush();
                    }
                    entry.getLong(); // discard ledger id, we already have it
                    long entryId = entry.getLong();
                    entry.rewind();

                    long newoffset = entryLogger.addEntry(ledgerId, entry);
                    offsets.add(new EntryLocation(ledgerId, entryId, newoffset));

                }
            };
        }

        void flush() throws IOException {
            if (offsets.isEmpty()) {
                LOG.debug("Skipping entry log flushing, as there are no offset!");
                return;
            }

            // Before updating the index, we want to wait until all the compacted entries are flushed into the
            // entryLog
            try {
                entryLogger.flush();

                ledgerStorage.updateEntriesLocations(offsets);
            } finally {
                offsets.clear();
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
                                  LedgerManager ledgerManager,
                                  final CompactableLedgerStorage ledgerStorage)
        throws IOException {
        super("GarbageCollectorThread");

        this.entryLogger = ledgerStorage.getEntryLogger();
        this.ledgerStorage = ledgerStorage;

        this.gcWaitTime = conf.getGcWaitTime();
        this.isThrottleByBytes = conf.getIsThrottleByBytes();
        this.maxOutstandingRequests = conf.getCompactionMaxOutstandingRequests();
        this.compactionRateByEntries  = conf.getCompactionRateByEntries();
        this.compactionRateByBytes = conf.getCompactionRateByBytes();
        this.scannerFactory = new CompactionScannerFactory();

        this.garbageCleaner = new GarbageCollector.GarbageCleaner() {
            @Override
            public void clean(long ledgerId) {
                try {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("delete ledger : " + ledgerId);
                    }

                    ledgerStorage.deleteLedger(ledgerId);
                } catch (IOException e) {
                    LOG.error("Exception when deleting the ledger index file on the Bookie: ", e);
                }
            }
        };

        this.garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, ledgerStorage, conf);

        // compaction parameters
        minorCompactionThreshold = conf.getMinorCompactionThreshold();
        minorCompactionInterval = conf.getMinorCompactionInterval() * SECOND;
        majorCompactionThreshold = conf.getMajorCompactionThreshold();
        majorCompactionInterval = conf.getMajorCompactionInterval() * SECOND;
        isForceGCAllowWhenNoSpace = conf.getIsForceGCAllowWhenNoSpace();

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

    public synchronized void enableForceGC() {
        if (forceGarbageCollection.compareAndSet(false, true)) {
            LOG.info("Forced garbage collection triggered by thread: {}", Thread.currentThread().getName());
            notify();
        }
    }

    public void disableForceGC() {
        if (forceGarbageCollection.compareAndSet(true, false)) {
            LOG.info("{} disabled force garbage collection since bookie has enough space now.", Thread
                    .currentThread().getName());
        }
    }

    public void suspendMajorGC() {
        if (suspendMajorCompaction.compareAndSet(false, true)) {
            LOG.info("Suspend Major Compaction triggered by thread: {}", Thread.currentThread().getName());
        }
    }

    public void resumeMajorGC() {
        if (suspendMajorCompaction.compareAndSet(true, false)) {
            LOG.info("{} Major Compaction back to normal since bookie has enough space now.", Thread.currentThread().getName());
        }
    }

    public void suspendMinorGC() {
        if (suspendMinorCompaction.compareAndSet(false, true)) {
            LOG.info("Suspend Minor Compaction triggered by thread: {}", Thread.currentThread().getName());
        }
    }

    public void resumeMinorGC() {
        if (suspendMinorCompaction.compareAndSet(true, false)) {
            LOG.info("{} Minor Compaction back to normal since bookie has enough space now.", Thread.currentThread().getName());
        }
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

            boolean force = forceGarbageCollection.get();
            if (force) {
                LOG.info("Garbage collector thread forced to perform GC before expiry of wait time.");
            }

            // Extract all of the ledger ID's that comprise all of the entry logs
            // (except for the current new one which is still being written to).
            entryLogMetaMap = extractMetaFromEntryLogs(entryLogMetaMap);

            // gc inactive/deleted ledgers
            doGcLedgers();

            // gc entry logs
            doGcEntryLogs();

            boolean suspendMajor = suspendMajorCompaction.get();
            boolean suspendMinor = suspendMinorCompaction.get();
            if (suspendMajor) {
                LOG.info("Disk almost full, suspend major compaction to slow down filling disk.");
            }
            if (suspendMinor) {
                LOG.info("Disk full, suspend minor compaction to slow down filling disk.");
            }

            long curTime = MathUtils.now();
            if (enableMajorCompaction && (!suspendMajor) &&
                (force || curTime - lastMajorCompactionTime > majorCompactionInterval)) {
                // enter major compaction
                LOG.info("Enter major compaction, suspendMajor {}", suspendMajor);
                doCompactEntryLogs(majorCompactionThreshold);
                lastMajorCompactionTime = MathUtils.now();
                // also move minor compaction time
                lastMinorCompactionTime = lastMajorCompactionTime;
                continue;
            }

            if (enableMinorCompaction && (!suspendMinor) &&
                (force || curTime - lastMinorCompactionTime > minorCompactionInterval)) {
                // enter minor compaction
                LOG.info("Enter minor compaction, suspendMinor {}", suspendMinor);
                doCompactEntryLogs(minorCompactionThreshold);
                lastMinorCompactionTime = MathUtils.now();
            }
            forceGarbageCollection.set(false);
        }
        LOG.info("GarbageCollectorThread exited loop!");
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
            for (Long entryLogLedger : meta.getLedgersMap().keySet()) {
                // Remove the entry log ledger from the set if it isn't active.
                try {
                    if (!ledgerStorage.ledgerExists(entryLogLedger)) {
                        meta.removeLedger(entryLogLedger);
                    }
                } catch (IOException e) {
                    LOG.error("Error reading from ledger storage", e);
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
    @VisibleForTesting
    void doCompactEntryLogs(double threshold) {
        LOG.info("Do compaction to compact those files lower than " + threshold);
        // sort the ledger meta by occupied unused space
        Comparator<EntryLogMetadata> sizeComparator = new Comparator<EntryLogMetadata>() {
            @Override
            public int compare(EntryLogMetadata m1, EntryLogMetadata m2) {
                long unusedSize1 = m1.getTotalSize() - m1.getRemainingSize();
                long unusedSize2 = m2.getTotalSize() - m2.getRemainingSize();
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

            if (LOG.isDebugEnabled()) {
                LOG.debug("Compacting entry log {} below threshold {}", meta.getEntryLogId(), threshold);
            }
            try {
                compactEntryLog(scannerFactory, meta);
                scannerFactory.flush();

                LOG.info("Removing entry log {} after compaction", meta.getEntryLogId());
                removeEntryLog(meta.getEntryLogId());

            } catch (LedgerDirsManager.NoWritableLedgerDirException nwlde) {
                LOG.warn("No writable ledger directory available, aborting compaction", nwlde);
                break;
            } catch (IOException ioe) {
                // if compact entry log throws IOException, we don't want to remove that
                // entry log. however, if some entries from that log have been readded
                // to the entry log, and the offset updated, it's ok to flush that
                LOG.error("Error compacting entry log. Log won't be deleted", ioe);
            }

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
        LOG.info("Shutting down GarbageCollectorThread");
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
    protected void compactEntryLog(CompactionScannerFactory scannerFactory,
                                   EntryLogMetadata entryLogMeta) throws IOException {
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

        LOG.info("Compacting entry log : {} - Usage: {} %", entryLogMeta.getEntryLogId(), entryLogMeta.getUsage());

        try {
            entryLogger.scanEntryLog(entryLogMeta.getEntryLogId(),
                                     scannerFactory.newScanner(entryLogMeta));
        } finally {
            // clear compacting flag
            compacting.set(false);
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
        long curLogId = entryLogger.getLeastUnflushedLogId();
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

            LOG.info("Extracting entry log meta from entryLogId: {}", entryLogId);

            try {
                // Read through the entry log file and extract the entry log meta
                EntryLogMetadata entryLogMeta = entryLogger.getEntryLogMetadata(entryLogId);
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
}
