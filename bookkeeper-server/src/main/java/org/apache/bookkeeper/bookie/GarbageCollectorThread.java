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

import static org.apache.bookkeeper.util.BookKeeperConstants.METADATA_CACHE;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import lombok.Getter;

import org.apache.bookkeeper.bookie.BookieException.EntryLogMetadataMapException;
import org.apache.bookkeeper.bookie.GarbageCollector.GarbageCleaner;
import org.apache.bookkeeper.bookie.stats.GarbageCollectorStats;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.bookie.storage.ldb.PersistentEntryLogMetadataMap;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the garbage collector thread that runs in the background to
 * remove any entry log files that no longer contains any active ledger.
 */
public class GarbageCollectorThread extends SafeRunnable {
    private static final Logger LOG = LoggerFactory.getLogger(GarbageCollectorThread.class);
    private static final int SECOND = 1000;

    // Maps entry log files to the set of ledgers that comprise the file and the size usage per ledger
    private EntryLogMetadataMap entryLogMetaMap;

    private final ScheduledExecutorService gcExecutor;
    Future<?> scheduledFuture = null;

    // This is how often we want to run the Garbage Collector Thread (in milliseconds).
    final long gcWaitTime;

    // Compaction parameters
    boolean isForceMinorCompactionAllow = false;
    boolean enableMinorCompaction = false;
    final double minorCompactionThreshold;
    final long minorCompactionInterval;
    final long minorCompactionMaxTimeMillis;
    long lastMinorCompactionTime;

    boolean isForceMajorCompactionAllow = false;
    boolean enableMajorCompaction = false;
    final double majorCompactionThreshold;
    final long majorCompactionInterval;
    long majorCompactionMaxTimeMillis;
    long lastMajorCompactionTime;

    @Getter
    final boolean isForceGCAllowWhenNoSpace;

    // Entry Logger Handle
    final EntryLogger entryLogger;
    final AbstractLogCompactor compactor;

    // Stats loggers for garbage collection operations
    private final GarbageCollectorStats gcStats;

    private volatile long totalEntryLogSize;
    private volatile int numActiveEntryLogs;

    final CompactableLedgerStorage ledgerStorage;

    // flag to ensure gc thread will not be interrupted during compaction
    // to reduce the risk getting entry log corrupted
    final AtomicBoolean compacting = new AtomicBoolean(false);

    // use to get the compacting status
    final AtomicBoolean minorCompacting = new AtomicBoolean(false);
    final AtomicBoolean majorCompacting = new AtomicBoolean(false);

    volatile boolean running = true;

    // track the last scanned successfully log id
    long scannedLogId = 0;

    // Boolean to trigger a forced GC.
    final AtomicBoolean forceGarbageCollection = new AtomicBoolean(false);
    // Boolean to disable major compaction, when disk is almost full
    final AtomicBoolean suspendMajorCompaction = new AtomicBoolean(false);
    // Boolean to disable minor compaction, when disk is full
    final AtomicBoolean suspendMinorCompaction = new AtomicBoolean(false);

    final ScanAndCompareGarbageCollector garbageCollector;
    final GarbageCleaner garbageCleaner;

    final ServerConfiguration conf;
    final LedgerDirsManager ledgerDirsManager;

    private static final AtomicLong threadNum = new AtomicLong(0);
    final AbstractLogCompactor.Throttler throttler;
    /**
     * Create a garbage collector thread.
     *
     * @param conf
     *          Server Configuration Object.
     * @throws IOException
     */
    public GarbageCollectorThread(ServerConfiguration conf, LedgerManager ledgerManager,
                                  final LedgerDirsManager ledgerDirsManager,
                                  final CompactableLedgerStorage ledgerStorage,
                                  StatsLogger statsLogger) throws IOException {
        this(conf, ledgerManager, ledgerDirsManager, ledgerStorage, statsLogger,
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("GarbageCollectorThread")));
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
                                  final LedgerDirsManager ledgerDirsManager,
                                  final CompactableLedgerStorage ledgerStorage,
                                  StatsLogger statsLogger,
                                  ScheduledExecutorService gcExecutor)
        throws IOException {
        this.gcExecutor = gcExecutor;
        this.conf = conf;

        this.ledgerDirsManager = ledgerDirsManager;
        this.entryLogger = ledgerStorage.getEntryLogger();
        this.entryLogMetaMap = createEntryLogMetadataMap();
        this.ledgerStorage = ledgerStorage;
        this.gcWaitTime = conf.getGcWaitTime();

        this.numActiveEntryLogs = 0;
        this.totalEntryLogSize = 0L;
        this.garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, ledgerStorage, conf, statsLogger);
        this.gcStats = new GarbageCollectorStats(
            statsLogger,
            () -> numActiveEntryLogs,
            () -> totalEntryLogSize,
            () -> garbageCollector.getNumActiveLedgers()
        );

        this.garbageCleaner = ledgerId -> {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("delete ledger : " + ledgerId);
                }
                gcStats.getDeletedLedgerCounter().inc();
                ledgerStorage.deleteLedger(ledgerId);
            } catch (IOException e) {
                LOG.error("Exception when deleting the ledger index file on the Bookie: ", e);
            }
        };

        // compaction parameters
        minorCompactionThreshold = conf.getMinorCompactionThreshold();
        minorCompactionInterval = conf.getMinorCompactionInterval() * SECOND;
        majorCompactionThreshold = conf.getMajorCompactionThreshold();
        majorCompactionInterval = conf.getMajorCompactionInterval() * SECOND;
        isForceGCAllowWhenNoSpace = conf.getIsForceGCAllowWhenNoSpace();
        majorCompactionMaxTimeMillis = conf.getMajorCompactionMaxTimeMillis();
        minorCompactionMaxTimeMillis = conf.getMinorCompactionMaxTimeMillis();

        boolean isForceAllowCompaction = conf.isForceAllowCompaction();

        AbstractLogCompactor.LogRemovalListener remover = new AbstractLogCompactor.LogRemovalListener() {
            @Override
            public void removeEntryLog(long logToRemove) {
                try {
                    GarbageCollectorThread.this.removeEntryLog(logToRemove);
                } catch (EntryLogMetadataMapException e) {
                    // Ignore and continue because ledger will not be cleaned up
                    // from entry-logger in this pass and will be taken care in
                    // next schedule task
                    LOG.warn("Failed to remove entry-log metadata {}", logToRemove, e);
                }
            }
        };
        if (conf.getUseTransactionalCompaction()) {
            this.compactor = new TransactionalEntryLogCompactor(conf, entryLogger, ledgerStorage,
                    ledgerDirsManager, remover);
        } else {
            this.compactor = new EntryLogCompactor(conf, entryLogger, ledgerStorage, remover);
        }

        this.throttler = new AbstractLogCompactor.Throttler(conf);
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

        if (isForceAllowCompaction) {
            if (minorCompactionThreshold > 0 && minorCompactionThreshold < 1.0f) {
                isForceMinorCompactionAllow = true;
            }
            if (majorCompactionThreshold > 0 && majorCompactionThreshold < 1.0f) {
                isForceMajorCompactionAllow = true;
            }
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
            if (minorCompactionInterval >= majorCompactionInterval
                || minorCompactionThreshold >= majorCompactionThreshold) {
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

    private EntryLogMetadataMap createEntryLogMetadataMap() throws IOException {
        if (conf.isGcEntryLogMetadataCacheEnabled()) {
            String baseDir = Strings.isNullOrEmpty(conf.getGcEntryLogMetadataCachePath())
                ? this.ledgerDirsManager.getAllLedgerDirs().get(0).getPath() : conf.getGcEntryLogMetadataCachePath();
            try {
                return new PersistentEntryLogMetadataMap(baseDir, conf);
            } catch (IOException e) {
                LOG.error("Failed to initialize persistent-metadata-map , clean up {}",
                    baseDir + "/" + METADATA_CACHE, e);
                throw e;
            }
        } else {
            return new InMemoryEntryLogMetadataMap();
        }
    }

    public void enableForceGC() {
        if (forceGarbageCollection.compareAndSet(false, true)) {
            LOG.info("Forced garbage collection triggered by thread: {}", Thread.currentThread().getName());
            triggerGC(true, suspendMajorCompaction.get(),
                      suspendMinorCompaction.get());
        }
    }

    public void enableForceGC(Boolean forceMajor, Boolean forceMinor) {
        if (forceGarbageCollection.compareAndSet(false, true)) {
            LOG.info("Forced garbage collection triggered by thread: {}, forceMajor: {}, forceMinor: {}",
                Thread.currentThread().getName(), forceMajor, forceMinor);
            triggerGC(true, forceMajor == null ? suspendMajorCompaction.get() : !forceMajor,
                forceMinor == null ? suspendMinorCompaction.get() : !forceMinor);
        }
    }

    public void disableForceGC() {
        if (forceGarbageCollection.compareAndSet(true, false)) {
            LOG.info("{} disabled force garbage collection since bookie has enough space now.", Thread
                    .currentThread().getName());
        }
    }

    Future<?> triggerGC(final boolean force,
                        final boolean suspendMajor,
                        final boolean suspendMinor) {
        return gcExecutor.submit(() -> {
                runWithFlags(force, suspendMajor, suspendMinor);
            });
    }

    Future<?> triggerGC() {
        final boolean force = forceGarbageCollection.get();
        final boolean suspendMajor = suspendMajorCompaction.get();
        final boolean suspendMinor = suspendMinorCompaction.get();

        return gcExecutor.submit(() -> {
                runWithFlags(force, suspendMajor, suspendMinor);
            });
    }

    public boolean isInForceGC() {
        return forceGarbageCollection.get();
    }

    public void suspendMajorGC() {
        if (suspendMajorCompaction.compareAndSet(false, true)) {
            LOG.info("Suspend Major Compaction triggered by thread: {}", Thread.currentThread().getName());
        }
    }

    public void resumeMajorGC() {
        if (suspendMajorCompaction.compareAndSet(true, false)) {
            LOG.info("{} Major Compaction back to normal since bookie has enough space now.",
                    Thread.currentThread().getName());
        }
    }

    public void suspendMinorGC() {
        if (suspendMinorCompaction.compareAndSet(false, true)) {
            LOG.info("Suspend Minor Compaction triggered by thread: {}", Thread.currentThread().getName());
        }
    }

    public void resumeMinorGC() {
        if (suspendMinorCompaction.compareAndSet(true, false)) {
            LOG.info("{} Minor Compaction back to normal since bookie has enough space now.",
                    Thread.currentThread().getName());
        }
    }

    public void start() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        long initialDelay = getModInitialDelay();
        scheduledFuture = gcExecutor.scheduleAtFixedRate(this, initialDelay, gcWaitTime, TimeUnit.MILLISECONDS);
    }

    /**
     * when number of ledger's Dir are more than 1,the same of GarbageCollectorThread will do the same thing,
     * Especially
     * 1) deleting ledger, then SyncThread will be timed to do rocksDB compact
     * 2) compact: entry, cost cpu.
     * then get Mod initial Delay time to simply avoid GarbageCollectorThread working at the same time
     */
    public long getModInitialDelay() {
        int ledgerDirsNum = conf.getLedgerDirs().length;
        long splitTime = gcWaitTime / ledgerDirsNum;
        long currentThreadNum = threadNum.incrementAndGet();
        return gcWaitTime + currentThreadNum * splitTime;
    }

    @Override
    public void safeRun() {
        boolean force = forceGarbageCollection.get();
        boolean suspendMajor = suspendMajorCompaction.get();
        boolean suspendMinor = suspendMinorCompaction.get();

        runWithFlags(force, suspendMajor, suspendMinor);

        if (force) {
            // only set force to false if it had been true when the garbage
            // collection cycle started
            forceGarbageCollection.set(false);
        }
    }

    public void runWithFlags(boolean force, boolean suspendMajor, boolean suspendMinor) {
        long threadStart = MathUtils.nowInNano();
        if (force) {
            LOG.info("Garbage collector thread forced to perform GC before expiry of wait time.");
        }
        // Recover and clean up previous state if using transactional compaction
        compactor.cleanUpAndRecover();

        try {
         // Extract all of the ledger ID's that comprise all of the entry logs
            // (except for the current new one which is still being written to).
            extractMetaFromEntryLogs();

            // gc inactive/deleted ledgers
            doGcLedgers();

            // gc entry logs
            doGcEntryLogs();

            if (suspendMajor) {
                LOG.info("Disk almost full, suspend major compaction to slow down filling disk.");
            }
            if (suspendMinor) {
                LOG.info("Disk full, suspend minor compaction to slow down filling disk.");
            }

            long curTime = System.currentTimeMillis();
            if (((isForceMajorCompactionAllow && force) || (enableMajorCompaction
                    && (force || curTime - lastMajorCompactionTime > majorCompactionInterval)))
                    && (!suspendMajor)) {
                // enter major compaction
                LOG.info("Enter major compaction, suspendMajor {}", suspendMajor);
                majorCompacting.set(true);
                try {
                    doCompactEntryLogs(majorCompactionThreshold, majorCompactionMaxTimeMillis);
                } finally {
                    lastMajorCompactionTime = System.currentTimeMillis();
                    // and also move minor compaction time
                    lastMinorCompactionTime = lastMajorCompactionTime;
                    gcStats.getMajorCompactionCounter().inc();
                    majorCompacting.set(false);
                }
            } else if (((isForceMinorCompactionAllow && force) || (enableMinorCompaction
                    && (force || curTime - lastMinorCompactionTime > minorCompactionInterval)))
                    && (!suspendMinor)) {
                // enter minor compaction
                LOG.info("Enter minor compaction, suspendMinor {}", suspendMinor);
                minorCompacting.set(true);
                try {
                    doCompactEntryLogs(minorCompactionThreshold, minorCompactionMaxTimeMillis);
                } finally {
                    lastMinorCompactionTime = System.currentTimeMillis();
                    gcStats.getMinorCompactionCounter().inc();
                    minorCompacting.set(false);
                }
            }
            gcStats.getGcThreadRuntime().registerSuccessfulEvent(
                    MathUtils.nowInNano() - threadStart, TimeUnit.NANOSECONDS);
        } catch (EntryLogMetadataMapException e) {
            LOG.error("Error in entryLog-metadatamap, Failed to complete GC/Compaction due to entry-log {}",
                    e.getMessage(), e);
            gcStats.getGcThreadRuntime().registerFailedEvent(
                    MathUtils.nowInNano() - threadStart, TimeUnit.NANOSECONDS);
        } finally {
            if (force && forceGarbageCollection.compareAndSet(true, false)) {
                LOG.info("{} Set forceGarbageCollection to false after force GC to make it forceGC-able again.",
                        Thread.currentThread().getName());
            }
        }

    }

    /**
     * Do garbage collection ledger index files.
     */
    private void doGcLedgers() {
        garbageCollector.gc(garbageCleaner);
    }

    /**
     * Garbage collect those entry loggers which are not associated with any active ledgers.
     */
    private void doGcEntryLogs() throws EntryLogMetadataMapException {
        // Get a cumulative count, don't update until complete
        AtomicLong totalEntryLogSizeAcc = new AtomicLong(0L);

        // Loop through all of the entry logs and remove the non-active ledgers.
        entryLogMetaMap.forEach((entryLogId, meta) -> {
            try {
                boolean modified = removeIfLedgerNotExists(meta);
                if (meta.isEmpty()) {
                    // This means the entry log is not associated with any active
                    // ledgers anymore.
                    // We can remove this entry log file now.
                    LOG.info("Deleting entryLogId {} as it has no active ledgers!", entryLogId);
                    removeEntryLog(entryLogId);
                    gcStats.getReclaimedSpaceViaDeletes().add(meta.getTotalSize());
                } else if (modified) {
                    // update entryLogMetaMap only when the meta modified.
                    entryLogMetaMap.put(meta.getEntryLogId(), meta);
                }
            } catch (EntryLogMetadataMapException e) {
                // Ignore and continue because ledger will not be cleaned up
                // from entry-logger in this pass and will be taken care in next
                // schedule task
                LOG.warn("Failed to remove ledger from entry-log metadata {}", entryLogId, e);
            }
           totalEntryLogSizeAcc.getAndAdd(meta.getRemainingSize());
        });

        this.totalEntryLogSize = totalEntryLogSizeAcc.get();
        this.numActiveEntryLogs = entryLogMetaMap.size();
    }

    private boolean removeIfLedgerNotExists(EntryLogMetadata meta) throws EntryLogMetadataMapException {
        MutableBoolean modified = new MutableBoolean(false);
        meta.removeLedgerIf((entryLogLedger) -> {
            // Remove the entry log ledger from the set if it isn't active.
            try {
                boolean exist = ledgerStorage.ledgerExists(entryLogLedger);
                if (!exist) {
                    modified.setTrue();
                }
                return !exist;
            } catch (IOException e) {
                LOG.error("Error reading from ledger storage", e);
                return false;
            }
        });

        return modified.getValue();
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
    void doCompactEntryLogs(double threshold, long maxTimeMillis) throws EntryLogMetadataMapException {
        LOG.info("Do compaction to compact those files lower than {}", threshold);

        final int numBuckets = 10;
        int[] entryLogUsageBuckets = new int[numBuckets];
        int[] compactedBuckets = new int[numBuckets];

        long start = System.currentTimeMillis();
        MutableLong end = new MutableLong(start);
        MutableLong timeDiff = new MutableLong(0);

        entryLogMetaMap.forEach((entryLogId, meta) -> {
            int bucketIndex = calculateUsageIndex(numBuckets, meta.getUsage());
            entryLogUsageBuckets[bucketIndex]++;

            if (timeDiff.getValue() < maxTimeMillis) {
                end.setValue(System.currentTimeMillis());
                timeDiff.setValue(end.getValue() - start);
            }
            if (meta.getUsage() >= threshold || (maxTimeMillis > 0 && timeDiff.getValue() >= maxTimeMillis)
                    || !running) {
                // We allow the usage limit calculation to continue so that we get a accurate
                // report of where the usage was prior to running compaction.
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Compacting entry log {} with usage {} below threshold {}",
                        meta.getEntryLogId(), meta.getUsage(), threshold);
            }

            long priorRemainingSize = meta.getRemainingSize();
            compactEntryLog(meta);
            gcStats.getReclaimedSpaceViaCompaction().add(meta.getTotalSize() - priorRemainingSize);
            compactedBuckets[bucketIndex]++;
        });
        if (LOG.isDebugEnabled()) {
            if (!running) {
                LOG.debug("Compaction exited due to gc not running");
            }
            if (timeDiff.getValue() > maxTimeMillis) {
                LOG.debug("Compaction ran for {}ms but was limited by {}ms", timeDiff, maxTimeMillis);
            }
        }
        LOG.info(
                "Compaction: entry log usage buckets[10% 20% 30% 40% 50% 60% 70% 80% 90% 100%] = {}, compacted {}",
                entryLogUsageBuckets, compactedBuckets);
    }

    /**
     * Calculate the index for the batch based on the usage between 0 and 1.
     *
     * @param numBuckets Number of reporting buckets.
     * @param usage 0.0 - 1.0 value representing the usage of the entry log.
     * @return index based on the number of buckets The last bucket will have the 1.0 if added.
     */
    int calculateUsageIndex(int numBuckets, double usage) {
        return Math.min(
                numBuckets - 1,
                (int) Math.floor(usage * numBuckets));
    }

    /**
     * Shutdown the garbage collector thread.
     *
     * @throws InterruptedException if there is an exception stopping gc thread.
     */
    @SuppressFBWarnings("SWL_SLEEP_WITH_LOCK_HELD")
    public synchronized void shutdown() throws InterruptedException {
        if (!this.running) {
            return;
        }
        LOG.info("Shutting down GarbageCollectorThread");

        while (!compacting.compareAndSet(false, true)) {
            // Wait till the thread stops compacting
            Thread.sleep(100);
        }

        this.running = false;
        // Interrupt GC executor thread
        gcExecutor.shutdownNow();
        try {
            entryLogMetaMap.close();
        } catch (Exception e) {
            LOG.warn("Failed to close entryLog metadata-map", e);
        }
    }

    /**
     * Remove entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     * @throws EntryLogMetadataMapException
     */
    protected void removeEntryLog(long entryLogId) throws EntryLogMetadataMapException {
        // remove entry log file successfully
        if (entryLogger.removeEntryLog(entryLogId)) {
            LOG.info("Removing entry log metadata for {}", entryLogId);
            entryLogMetaMap.remove(entryLogId);
        }
    }

    /**
     * Compact an entry log.
     *
     * @param entryLogMeta
     */
    protected void compactEntryLog(EntryLogMetadata entryLogMeta) {
        // Similar with Sync Thread
        // try to mark compacting flag to make sure it would not be interrupted
        // by shutdown during compaction. otherwise it will receive
        // ClosedByInterruptException which may cause index file & entry logger
        // closed and corrupted.
        if (!compacting.compareAndSet(false, true)) {
            // set compacting flag failed, means compacting is true now
            // indicates that compaction is in progress for this EntryLogId.
            return;
        }

        try {
            // Do the actual compaction
            compactor.compact(entryLogMeta);
        } catch (Exception e) {
            LOG.error("Failed to compact entry log {} due to unexpected error", entryLogMeta.getEntryLogId(), e);
        } finally {
            // Mark compaction done
            compacting.set(false);
        }
    }

    /**
     * Method to read in all of the entry logs (those that we haven't done so yet),
     * and find the set of ledger ID's that make up each entry log file.
     *
     * @throws EntryLogMetadataMapException
     */
    protected void extractMetaFromEntryLogs() throws EntryLogMetadataMapException {
        // Entry Log ID's are just a long value that starts at 0 and increments by 1 when the log fills up and we roll
        // to a new one. We scan entry logs as follows:
        // - entryLogPerLedgerEnabled is false: Extract it for every entry log except for the current one (un-flushed).
        // - entryLogPerLedgerEnabled is true: Scan all flushed entry logs up to the highest known id.
        Supplier<Long> finalEntryLog = () -> conf.isEntryLogPerLedgerEnabled() ? entryLogger.getLastLogId() :
                entryLogger.getLeastUnflushedLogId();
        boolean hasExceptionWhenScan = false;
        boolean increaseScannedLogId = true;
        for (long entryLogId = scannedLogId; entryLogId < finalEntryLog.get(); entryLogId++) {
            // Comb the current entry log file if it has not already been extracted.
            if (entryLogMetaMap.containsKey(entryLogId)) {
                continue;
            }

            // check whether log file exists or not
            // if it doesn't exist, this log file might have been garbage collected.
            if (!entryLogger.logExists(entryLogId)) {
                continue;
            }

            // If entryLogPerLedgerEnabled is true, we will look for entry log files beyond getLeastUnflushedLogId()
            // that have been explicitly rotated or below getLeastUnflushedLogId().
            if (conf.isEntryLogPerLedgerEnabled() && !entryLogger.isFlushedEntryLog(entryLogId)) {
                LOG.info("Entry log {} not flushed (entryLogPerLedgerEnabled). Starting next iteration at this point.",
                        entryLogId);
                increaseScannedLogId = false;
                continue;
            }

            LOG.info("Extracting entry log meta from entryLogId: {}", entryLogId);

            try {
                // Read through the entry log file and extract the entry log meta
                EntryLogMetadata entryLogMeta = entryLogger.getEntryLogMetadata(entryLogId, throttler);
                removeIfLedgerNotExists(entryLogMeta);
                if (entryLogMeta.isEmpty()) {
                    entryLogger.removeEntryLog(entryLogId);
                    // remove it from entrylogmetadata-map if it is present in
                    // the map
                    entryLogMetaMap.remove(entryLogId);
                } else {
                    entryLogMetaMap.put(entryLogId, entryLogMeta);
                }
            } catch (IOException e) {
                hasExceptionWhenScan = true;
                LOG.warn("Premature exception when processing " + entryLogId
                         + " recovery will take care of the problem", e);
            }

            // if scan failed on some entry log, we don't move 'scannedLogId' to next id
            // if scan succeed, we don't need to scan it again during next gc run,
            // we move 'scannedLogId' to next id (unless entryLogPerLedgerEnabled is true
            // and we have found and un-flushed entry log already).
            if (!hasExceptionWhenScan && (!conf.isEntryLogPerLedgerEnabled() || increaseScannedLogId)) {
                ++scannedLogId;
            }
        }
    }

    CompactableLedgerStorage getLedgerStorage() {
        return ledgerStorage;
    }

    @VisibleForTesting
    EntryLogMetadataMap getEntryLogMetaMap() {
        return entryLogMetaMap;
    }

    public GarbageCollectionStatus getGarbageCollectionStatus() {
        return GarbageCollectionStatus.builder()
            .forceCompacting(forceGarbageCollection.get())
            .majorCompacting(majorCompacting.get())
            .minorCompacting(minorCompacting.get())
            .lastMajorCompactionTime(lastMajorCompactionTime)
            .lastMinorCompactionTime(lastMinorCompactionTime)
            .majorCompactionCounter(gcStats.getMajorCompactionCounter().get())
            .minorCompactionCounter(gcStats.getMinorCompactionCounter().get())
            .build();
    }
}
