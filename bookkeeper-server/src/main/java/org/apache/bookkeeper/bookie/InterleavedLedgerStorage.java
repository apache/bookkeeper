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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_READ_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ENTRYLOGGER_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.STORAGE_GET_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.STORAGE_GET_OFFSET;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.STORAGE_SCRUB_PAGES_SCANNED;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.STORAGE_SCRUB_PAGE_RETRIES;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.Getter;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.DefaultEntryLogger.EntryLogListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interleave ledger storage.
 *
 * <p>This ledger storage implementation stores all entries in a single
 * file and maintains an index file for each ledger.
 */
@StatsDoc(
    name = BOOKIE_SCOPE,
    category = CATEGORY_SERVER,
    help = "Bookie related stats"
)
public class InterleavedLedgerStorage implements CompactableLedgerStorage, EntryLogListener {
    private static final Logger LOG = LoggerFactory.getLogger(InterleavedLedgerStorage.class);

    DefaultEntryLogger entryLogger;
    @Getter
    LedgerCache ledgerCache;
    protected CheckpointSource checkpointSource = CheckpointSource.DEFAULT;
    protected Checkpointer checkpointer = Checkpointer.NULL;
    private final CopyOnWriteArrayList<LedgerDeletionListener> ledgerDeletionListeners =
            Lists.newCopyOnWriteArrayList();

    // A sorted map to stored all active ledger ids
    protected final SnapshotMap<Long, Boolean> activeLedgers;

    // This is the thread that garbage collects the entry logs that do not
    // contain any active ledgers in them; and compacts the entry logs that
    // has lower remaining percentage to reclaim disk space.
    GarbageCollectorThread gcThread;

    // this indicates that a write has happened since the last flush
    private final AtomicBoolean somethingWritten = new AtomicBoolean(false);

    // Expose Stats
    @StatsDoc(
        name = STORAGE_GET_OFFSET,
        help = "Operation stats of getting offset from ledger cache",
        parent = BOOKIE_READ_ENTRY
    )
    private OpStatsLogger getOffsetStats;
    @StatsDoc(
        name = STORAGE_GET_ENTRY,
        help = "Operation stats of getting entry from entry logger",
        parent = BOOKIE_READ_ENTRY,
        happensAfter = STORAGE_GET_OFFSET
    )
    private OpStatsLogger getEntryStats;
    private OpStatsLogger pageScanStats;
    private Counter retryCounter;

    public InterleavedLedgerStorage() {
        activeLedgers = new SnapshotMap<>();
    }

    @Override
    public void initialize(ServerConfiguration conf,
                           LedgerManager ledgerManager,
                           LedgerDirsManager ledgerDirsManager,
                           LedgerDirsManager indexDirsManager,
                           StatsLogger statsLogger,
                           ByteBufAllocator allocator)
            throws IOException {
        initializeWithEntryLogListener(
            conf,
            ledgerManager,
            ledgerDirsManager,
            indexDirsManager,
            this,
            statsLogger,
            allocator);
    }

    void initializeWithEntryLogListener(ServerConfiguration conf,
                                        LedgerManager ledgerManager,
                                        LedgerDirsManager ledgerDirsManager,
                                        LedgerDirsManager indexDirsManager,
                                        EntryLogListener entryLogListener,
                                        StatsLogger statsLogger,
                                        ByteBufAllocator allocator) throws IOException {
        initializeWithEntryLogger(
                conf,
                ledgerManager,
                ledgerDirsManager,
                indexDirsManager,
                new DefaultEntryLogger(conf, ledgerDirsManager, entryLogListener, statsLogger.scope(ENTRYLOGGER_SCOPE),
                        allocator),
                statsLogger);
    }

    @Override
    public void setStateManager(StateManager stateManager) {}

    @Override
    public void setCheckpointSource(CheckpointSource checkpointSource) {
        this.checkpointSource = checkpointSource;
    }

    @Override
    public void setCheckpointer(Checkpointer checkpointer) {
        this.checkpointer = checkpointer;
    }

    public void initializeWithEntryLogger(ServerConfiguration conf,
                LedgerManager ledgerManager,
                LedgerDirsManager ledgerDirsManager,
                LedgerDirsManager indexDirsManager,
                EntryLogger entryLogger,
                StatsLogger statsLogger) throws IOException {
        checkNotNull(checkpointSource, "invalid null checkpoint source");
        checkNotNull(checkpointer, "invalid null checkpointer");
        this.entryLogger = (DefaultEntryLogger) entryLogger;
        this.entryLogger.addListener(this);
        ledgerCache = new LedgerCacheImpl(conf, activeLedgers,
                null == indexDirsManager ? ledgerDirsManager : indexDirsManager, statsLogger);
        gcThread = new GarbageCollectorThread(conf, ledgerManager, ledgerDirsManager,
                                              this, entryLogger, statsLogger.scope("gc"));
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        // Expose Stats
        getOffsetStats = statsLogger.getOpStatsLogger(STORAGE_GET_OFFSET);
        getEntryStats = statsLogger.getOpStatsLogger(STORAGE_GET_ENTRY);
        pageScanStats = statsLogger.getOpStatsLogger(STORAGE_SCRUB_PAGES_SCANNED);
        retryCounter = statsLogger.getCounter(STORAGE_SCRUB_PAGE_RETRIES);
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {

            @Override
            public void diskAlmostFull(File disk) {
                if (gcThread.isForceGCAllowWhenNoSpace) {
                    gcThread.enableForceGC();
                } else {
                    gcThread.suspendMajorGC();
                }
            }

            @Override
            public void diskFull(File disk) {
                if (gcThread.isForceGCAllowWhenNoSpace) {
                    gcThread.enableForceGC();
                } else {
                    gcThread.suspendMajorGC();
                    gcThread.suspendMinorGC();
                }
            }

            @Override
            public void allDisksFull(boolean highPriorityWritesAllowed) {
                if (gcThread.isForceGCAllowWhenNoSpace) {
                    gcThread.enableForceGC();
                } else {
                    gcThread.suspendMajorGC();
                    gcThread.suspendMinorGC();
                }
            }

            @Override
            public void diskWritable(File disk) {
                // we have enough space now
                if (gcThread.isForceGCAllowWhenNoSpace) {
                    // disable force gc.
                    gcThread.disableForceGC();
                } else {
                    // resume compaction to normal.
                    gcThread.resumeMajorGC();
                    gcThread.resumeMinorGC();
                }
            }

            @Override
            public void diskJustWritable(File disk) {
                if (gcThread.isForceGCAllowWhenNoSpace) {
                    // if a disk is just writable, we still need force gc.
                    gcThread.enableForceGC();
                } else {
                    // still under warn threshold, only resume minor compaction.
                    gcThread.resumeMinorGC();
                }
            }
        };
    }

    @Override
    public void forceGC() {
        gcThread.enableForceGC();
    }

    @Override
    public void forceGC(boolean forceMajor, boolean forceMinor) {
        gcThread.enableForceGC(forceMajor, forceMinor);
    }

    @Override
    public boolean isInForceGC() {
        return gcThread.isInForceGC();
    }

    public void suspendMinorGC() {
        gcThread.suspendMinorGC();
    }

    public void suspendMajorGC() {
        gcThread.suspendMajorGC();
    }

    public void resumeMinorGC() {
        gcThread.resumeMinorGC();
    }

    public void resumeMajorGC() {
        gcThread.resumeMajorGC();
    }

    public boolean isMajorGcSuspended() {
        return gcThread.isMajorGcSuspend();
    }

    public boolean isMinorGcSuspended() {
        return gcThread.isMinorGcSuspend();
    }

    @Override
    public void start() {
        gcThread.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        // shut down gc thread, which depends on zookeeper client
        // also compaction will write entries again to entry log file
        LOG.info("Shutting down InterleavedLedgerStorage");
        LOG.info("Shutting down GC thread");
        gcThread.shutdown();
        LOG.info("Shutting down entry logger");
        entryLogger.close();
        try {
            ledgerCache.close();
        } catch (IOException e) {
            LOG.error("Error while closing the ledger cache", e);
        }
        LOG.info("Complete shutting down Ledger Storage");
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        return ledgerCache.setFenced(ledgerId);
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        return ledgerCache.isFenced(ledgerId);
    }

    @Override
    public void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        ledgerCache.setExplicitLac(ledgerId, lac);
    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) {
        return ledgerCache.getExplicitLac(ledgerId);
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        ledgerCache.setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return ledgerCache.readMasterKey(ledgerId);
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        return ledgerCache.ledgerExists(ledgerId);
    }

    @Override
    public boolean entryExists(long ledgerId, long entryId) throws IOException {
        //Implementation should be as simple as what's below, but this needs testing
        //return ledgerCache.getEntryOffset(ledgerId, entryId) > 0;
        throw new UnsupportedOperationException("entry exists not supported");
    }

    @Override
    public long getLastAddConfirmed(long ledgerId) throws IOException {
        Long lac = ledgerCache.getLastAddConfirmed(ledgerId);
        if (lac == null) {
            ByteBuf bb = getEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED);
            if (null == bb) {
                return BookieProtocol.INVALID_ENTRY_ID;
            } else {
                try {
                    bb.skipBytes(2 * Long.BYTES); // skip ledger & entry id
                    lac = bb.readLong();
                    lac = ledgerCache.updateLastAddConfirmed(ledgerId, lac);
                } finally {
                    ReferenceCountUtil.release(bb);
                }
            }
        }
        return lac;
    }

    @Override
    public boolean waitForLastAddConfirmedUpdate(long ledgerId,
                                                 long previousLAC,
                                                 Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        return ledgerCache.waitForLastAddConfirmedUpdate(ledgerId, previousLAC, watcher);
    }

    @Override
    public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                    Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        ledgerCache.cancelWaitForLastAddConfirmedUpdate(ledgerId, watcher);
    }

    @Override
    public long addEntry(ByteBuf entry) throws IOException {
        long ledgerId = entry.getLong(entry.readerIndex() + 0);
        long entryId = entry.getLong(entry.readerIndex() + 8);
        long lac = entry.getLong(entry.readerIndex() + 16);

        processEntry(ledgerId, entryId, entry);

        ledgerCache.updateLastAddConfirmed(ledgerId, lac);
        return entryId;
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException {
        long offset;
        /*
         * If entryId is BookieProtocol.LAST_ADD_CONFIRMED, then return the last written.
         */
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            entryId = ledgerCache.getLastEntry(ledgerId);
        }

        // Get Offset
        long startTimeNanos = MathUtils.nowInNano();
        boolean success = false;
        try {
            offset = ledgerCache.getEntryOffset(ledgerId, entryId);
            if (offset == 0) {
                throw new Bookie.NoEntryException(ledgerId, entryId);
            }
            success = true;
        } finally {
            if (success) {
                getOffsetStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            } else {
                getOffsetStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            }
        }
        // Get Entry
        startTimeNanos = MathUtils.nowInNano();
        success = false;
        try {
            ByteBuf retBytes = entryLogger.readEntry(ledgerId, entryId, offset);
            success = true;
            return retBytes;
        } finally {
            if (success) {
                getEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            } else {
                getEntryStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            }
        }
    }

    private void flushOrCheckpoint(boolean isCheckpointFlush)
            throws IOException {

        boolean flushFailed = false;
        try {
            ledgerCache.flushLedger(true);
        } catch (LedgerDirsManager.NoWritableLedgerDirException e) {
            throw e;
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger cache", ioe);
            flushFailed = true;
        }

        try {
            // if it is just a checkpoint flush, we just flush rotated entry log files
            // in entry logger.
            if (isCheckpointFlush) {
                entryLogger.checkpoint();
            } else {
                entryLogger.flush();
            }
        } catch (LedgerDirsManager.NoWritableLedgerDirException e) {
            throw e;
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger", ioe);
            flushFailed = true;
        }
        if (flushFailed) {
            throw new IOException("Flushing to storage failed, check logs");
        }
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws IOException {
        // we don't need to check somethingwritten since checkpoint
        // is scheduled when rotate an entry logger file. and we could
        // not set somethingWritten to false after checkpoint, since
        // current entry logger file isn't flushed yet.
        flushOrCheckpoint(true);
    }

    @Override
    public synchronized void flush() throws IOException {
        if (!somethingWritten.compareAndSet(true, false)) {
            return;
        }
        flushOrCheckpoint(false);
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        activeLedgers.remove(ledgerId);
        ledgerCache.deleteLedger(ledgerId);

        for (LedgerDeletionListener listener : ledgerDeletionListeners) {
            listener.ledgerDeleted(ledgerId);
        }
    }

    @Override
    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) {
        NavigableMap<Long, Boolean> bkActiveLedgersSnapshot = activeLedgers.snapshot();
        Map<Long, Boolean> subBkActiveLedgers = bkActiveLedgersSnapshot
                .subMap(firstLedgerId, true, lastLedgerId, false);

        return subBkActiveLedgers.keySet();
    }

    @Override
    public void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException {
        for (EntryLocation l : locations) {
            try {
                ledgerCache.putEntryOffset(l.ledger, l.entry, l.location);
            } catch (NoLedgerException e) {
                // Ledger was already deleted, we can skip it in the compaction
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Compaction failed for deleted ledger ledger: {} entry: {}", l.ledger, l.entry);
                }
            }
        }
    }

    @Override
    public void flushEntriesLocationsIndex() throws IOException {
        ledgerCache.flushLedger(true);
    }

    public DefaultEntryLogger getEntryLogger() {
        return entryLogger;
    }

    @Override
    public void registerLedgerDeletionListener(LedgerDeletionListener listener) {
        ledgerDeletionListeners.add(listener);
    }

    protected void processEntry(long ledgerId, long entryId, ByteBuf entry) throws IOException {
        processEntry(ledgerId, entryId, entry, true);
    }

    protected void processEntry(long ledgerId, long entryId, ByteBuf entry, boolean rollLog)
            throws IOException {
        /*
         * Touch dirty flag
         */
        somethingWritten.set(true);

        /*
         * Log the entry
         */
        long pos = entryLogger.addEntry(ledgerId, entry, rollLog);

        /*
         * Set offset of entry id to be the current ledger position
         */
        ledgerCache.putEntryOffset(ledgerId, entryId, pos);
    }

    @Override
    public void onRotateEntryLog() {
        // for interleaved ledger storage, we request a checkpoint when rotating a entry log file.
        // the checkpoint represent the point that all the entries added before this point are already
        // in ledger storage and ready to be synced to disk.
        // TODO: we could consider remove checkpointSource and checkpointSouce#newCheckpoint
        // later if we provide kind of LSN (Log/Journal Squeuence Number)
        // mechanism when adding entry. {@link https://github.com/apache/bookkeeper/issues/279}
        Checkpoint checkpoint = checkpointSource.newCheckpoint();
        checkpointer.startCheckpoint(checkpoint);
    }

    /**
     * Return iterable for index entries for ledgerId.
     * @param ledgerId ledger to scan
     * @return Iterator
     */
    public LedgerCache.PageEntriesIterable getIndexEntries(long ledgerId) throws IOException {
        return ledgerCache.listEntries(ledgerId);
    }

    /**
     * Read implementation metadata for index file.
     * @param ledgerId
     * @return Implementation metadata
     * @throws IOException
     */
    public LedgerCache.LedgerIndexMetadata readLedgerIndexMetadata(long ledgerId) throws IOException {
        return ledgerCache.readLedgerIndexMetadata(ledgerId);
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public List<DetectedInconsistency> localConsistencyCheck(Optional<RateLimiter> rateLimiter) throws IOException {
        long checkStart = MathUtils.nowInNano();
        LOG.info("Starting localConsistencyCheck");
        long checkedLedgers = 0;
        long checkedPages = 0;
        final MutableLong checkedEntries = new MutableLong(0);
        final MutableLong pageRetries = new MutableLong(0);
        NavigableMap<Long, Boolean> bkActiveLedgersSnapshot = activeLedgers.snapshot();
        final List<DetectedInconsistency> errors = new ArrayList<>();
        for (Long ledger : bkActiveLedgersSnapshot.keySet()) {
            try (LedgerCache.PageEntriesIterable pages = ledgerCache.listEntries(ledger)) {
                for (LedgerCache.PageEntries page : pages) {
                    @Cleanup LedgerEntryPage lep = page.getLEP();
                    MutableBoolean retry = new MutableBoolean(false);
                    do {
                        retry.setValue(false);
                        int version = lep.getVersion();

                        MutableBoolean success = new MutableBoolean(true);
                        long start = MathUtils.nowInNano();
                        lep.getEntries((entry, offset) -> {
                            rateLimiter.ifPresent(RateLimiter::acquire);

                            try {
                                entryLogger.checkEntry(ledger, entry, offset);
                                checkedEntries.increment();
                            } catch (DefaultEntryLogger.EntryLookupException e) {
                                if (version != lep.getVersion()) {
                                    pageRetries.increment();
                                    if (lep.isDeleted()) {
                                        if (LOG.isDebugEnabled()) {
                                            LOG.debug("localConsistencyCheck: ledger {} deleted",
                                                    ledger);
                                        }
                                    } else {
                                        if (LOG.isDebugEnabled()) {
                                            LOG.debug("localConsistencyCheck: "
                                                    + "concurrent modification, retrying");
                                        }
                                        retry.setValue(true);
                                        retryCounter.inc();
                                    }
                                    return false;
                                } else {
                                    errors.add(new DetectedInconsistency(ledger, entry, e));
                                    LOG.error("Got error: ", e);
                                }
                                success.setValue(false);
                            }
                            return true;
                        });

                        if (success.booleanValue()) {
                            pageScanStats.registerSuccessfulEvent(
                                MathUtils.elapsedNanos(start), TimeUnit.NANOSECONDS);
                        } else {
                            pageScanStats.registerFailedEvent(
                                MathUtils.elapsedNanos(start), TimeUnit.NANOSECONDS);
                        }
                    } while (retry.booleanValue());
                    checkedPages++;
                }
            } catch (NoLedgerException | FileInfo.FileInfoDeletedException e) {
                if (activeLedgers.containsKey(ledger)) {
                    LOG.error("Cannot find ledger {}, should exist, exception is ", ledger, e);
                    errors.add(new DetectedInconsistency(ledger, -1, e));
                } else if (LOG.isDebugEnabled()){
                    LOG.debug("ledger {} deleted since snapshot taken", ledger);
                }
            } catch (Exception e) {
                throw new IOException("Got other exception in localConsistencyCheck", e);
            }
            checkedLedgers++;
        }
        LOG.info(
            "Finished localConsistencyCheck, took {}s to scan {} ledgers, {} pages, "
                + "{} entries with {} retries, {} errors",
            TimeUnit.NANOSECONDS.toSeconds(MathUtils.elapsedNanos(checkStart)),
            checkedLedgers,
            checkedPages,
            checkedEntries.longValue(),
            pageRetries.longValue(),
            errors.size());

        return errors;
    }

    @Override
    public List<GarbageCollectionStatus> getGarbageCollectionStatus() {
        return Collections.singletonList(gcThread.getGarbageCollectionStatus());
    }

    @Override
    public OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
        return ledgerCache.getEntriesIterator(ledgerId);
    }

    @Override
    public void setLimboState(long ledgerId) throws IOException {
        throw new UnsupportedOperationException(
                "Limbo state only supported for DbLedgerStorage");
    }

    @Override
    public boolean hasLimboState(long ledgerId) throws IOException {
        throw new UnsupportedOperationException(
                "Limbo state only supported for DbLedgerStorage");
    }

    @Override
    public void clearLimboState(long ledgerId) throws IOException {
        throw new UnsupportedOperationException(
                "Limbo state only supported for DbLedgerStorage");
    }

    @Override
    public EnumSet<StorageState> getStorageStateFlags() throws IOException {
        return EnumSet.noneOf(StorageState.class);
    }

    @Override
    public void setStorageStateFlag(StorageState flags) throws IOException {
        throw new UnsupportedOperationException(
                "Storage state only flags supported for DbLedgerStorage");
    }

    @Override
    public void clearStorageStateFlag(StorageState flags) throws IOException {
        throw new UnsupportedOperationException(
                "Storage state flags only supported for DbLedgerStorage");
    }
}
