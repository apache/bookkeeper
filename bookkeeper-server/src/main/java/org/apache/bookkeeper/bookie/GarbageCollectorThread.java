/*
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
import java.util.Comparator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.bookie.EntryLogger.EntryLogMetadata;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.zookeeper.ZooKeeper;

/**
 * This is the garbage collector thread that runs in the background to
 * remove any entry log files that no longer contains any active ledger.
 */
public class GarbageCollectorThread extends Thread {
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

    long lastMinorCompactionTime;
    long lastMajorCompactionTime;

    // Entry Logger Handle
    final EntryLogger entryLogger;
    final EntryLogScanner scanner;

    // Ledger Cache Handle
    final LedgerCache ledgerCache;

    final LedgerManager ledgerManager;

    // ZooKeeper Client
    final ZooKeeper zk;

    // flag to ensure gc thread will not be interrupted during compaction
    // to reduce the risk getting entry log corrupted
    final AtomicBoolean compacting = new AtomicBoolean(false);

    volatile boolean running = true;

    /**
     * A scanner wrapper to check whether a ledger is alive in an entry log file
     */
    class CompactionScanner implements EntryLogScanner {
        EntryLogMetadata meta;

        public CompactionScanner(EntryLogMetadata meta) {
            this.meta = meta;
        }

        @Override
        public boolean accept(long ledgerId) {
            return meta.containsLedger(ledgerId) && scanner.accept(ledgerId);
        }

        @Override
        public void process(long ledgerId, ByteBuffer entry) throws IOException {
            scanner.process(ledgerId, entry);
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
                                  ZooKeeper zookeeper,
                                  LedgerCache ledgerCache,
                                  EntryLogger entryLogger,
                                  LedgerManager ledgerManager,
                                  EntryLogScanner scanner)
        throws IOException {
        super("GarbageCollectorThread");

        this.zk = zookeeper;
        this.ledgerCache = ledgerCache;
        this.entryLogger = entryLogger;
        this.ledgerManager = ledgerManager;
        this.scanner = scanner;

        this.gcWaitTime = conf.getGcWaitTime();
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

        lastMinorCompactionTime = lastMajorCompactionTime = System.currentTimeMillis();
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

            // Dependency check.
            if (null == zk) {
                continue;
            }

            // Extract all of the ledger ID's that comprise all of the entry logs
            // (except for the current new one which is still being written to).
            try {
                entryLogMetaMap = entryLogger.extractMetaFromEntryLogs(entryLogMetaMap);
            } catch (IOException ie) {
                LOG.warn("Exception when extracting entry log meta from entry logs : ", ie);
            }

            // gc inactive/deleted ledgers
            doGcLedgers();

            // gc entry logs
            doGcEntryLogs();

            long curTime = System.currentTimeMillis();
            if (enableMajorCompaction &&
                curTime - lastMajorCompactionTime > majorCompactionInterval) {
                // enter major compaction
                LOG.info("Enter major compaction");
                doCompactEntryLogs(majorCompactionThreshold);
                lastMajorCompactionTime = System.currentTimeMillis();
                // also move minor compaction time
                lastMinorCompactionTime = lastMajorCompactionTime;
                continue;
            }

            if (enableMinorCompaction &&
                curTime - lastMinorCompactionTime > minorCompactionInterval) {
                // enter minor compaction
                LOG.info("Enter minor compaction");
                doCompactEntryLogs(minorCompactionThreshold);
                lastMinorCompactionTime = System.currentTimeMillis();
            }
        }
    }

    /**
     * Do garbage collection ledger index files
     */
    private void doGcLedgers() {
        ledgerManager.garbageCollectLedgers(
        new LedgerManager.GarbageCollector() {
            @Override
            public void gc(long ledgerId) {
                try {
                    ledgerCache.deleteLedger(ledgerId);
                } catch (IOException e) {
                    LOG.error("Exception when deleting the ledger index file on the Bookie: ", e);
                }
            }
        });
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
                if (!ledgerManager.containsActiveLedger(entryLogLedger)) {
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
        List<EntryLogMetadata> logsToCompact = new ArrayList();
        logsToCompact.addAll(entryLogMetaMap.values());
        Collections.sort(logsToCompact, sizeComparator);
        for (EntryLogMetadata meta : logsToCompact) {
            if (meta.getUsage() >= threshold) {
                break;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Compacting entry log " + meta.entryLogId + " below threshold "
                        + threshold + ".");
            }
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
            entryLogger.scanEntryLog(entryLogId, new CompactionScanner(entryLogMeta));
            // after moving entries to new entry log, remove this old one
            removeEntryLog(entryLogId);
        } catch (IOException e) {
            LOG.info("Premature exception when compacting " + entryLogId, e);
        } finally {
            // clear compacting flag
            compacting.set(false);
        }
    }
}
