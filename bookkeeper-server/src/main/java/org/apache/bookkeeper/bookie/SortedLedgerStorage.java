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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.IteratorUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code SortedLedgerStorage} is an extension of {@link InterleavedLedgerStorage}. It
 * is comprised of two {@code MemTable}s and a {@code InterleavedLedgerStorage}. All the
 * entries will be first added into a {@code MemTable}, and then be flushed back to the
 * {@code InterleavedLedgerStorage} when the {@code MemTable} becomes full.
 */
public class SortedLedgerStorage
        implements LedgerStorage, CacheCallback, SkipListFlusher,
            CompactableLedgerStorage, DefaultEntryLogger.EntryLogListener {
    private static final Logger LOG = LoggerFactory.getLogger(SortedLedgerStorage.class);

    EntryMemTable memTable;
    private ScheduledExecutorService scheduler;
    private StateManager stateManager;
    private ServerConfiguration conf;
    private StatsLogger statsLogger;
    private final InterleavedLedgerStorage interleavedLedgerStorage;

    public SortedLedgerStorage() {
        this(new InterleavedLedgerStorage());
    }

    @VisibleForTesting
    protected SortedLedgerStorage(InterleavedLedgerStorage ils) {
        interleavedLedgerStorage = ils;
    }

    @Override
    public void initialize(ServerConfiguration conf,
                           LedgerManager ledgerManager,
                           LedgerDirsManager ledgerDirsManager,
                           LedgerDirsManager indexDirsManager,
                           StatsLogger statsLogger,
                           ByteBufAllocator allocator)
            throws IOException {
        this.conf = conf;
        this.statsLogger = statsLogger;

        interleavedLedgerStorage.initializeWithEntryLogListener(
            conf,
            ledgerManager,
            ledgerDirsManager,
            indexDirsManager,
            // uses sorted ledger storage's own entry log listener
            // since it manages entry log rotations and checkpoints.
            this,
            statsLogger,
            allocator);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setNameFormat("SortedLedgerStorage-%d")
                .setPriority((Thread.NORM_PRIORITY + Thread.MAX_PRIORITY) / 2).build());
    }

    @Override
    public void setStateManager(StateManager stateManager) {
        interleavedLedgerStorage.setStateManager(stateManager);
        this.stateManager = stateManager;
    }
    @Override
    public void setCheckpointSource(CheckpointSource checkpointSource) {
        interleavedLedgerStorage.setCheckpointSource(checkpointSource);

        if (conf.isEntryLogPerLedgerEnabled()) {
            this.memTable = new EntryMemTableWithParallelFlusher(conf, checkpointSource, statsLogger);
        } else {
            this.memTable = new EntryMemTable(conf, checkpointSource, statsLogger);
        }
    }
    @Override
    public void setCheckpointer(Checkpointer checkpointer) {
        interleavedLedgerStorage.setCheckpointer(checkpointer);
    }

    @VisibleForTesting
    ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    @Override
    public void start() {
        try {
            flush();
        } catch (IOException e) {
            LOG.error("Exception thrown while flushing ledger cache.", e);
        }
        interleavedLedgerStorage.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        // Wait for any jobs currently scheduled to be completed and then shut down.
        scheduler.shutdown();
        if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
            scheduler.shutdownNow();
        }
        try {
            memTable.close();
        } catch (Exception e) {
            LOG.error("Error while closing the memtable", e);
        }
        interleavedLedgerStorage.shutdown();
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        // Done this way because checking the skip list is an O(logN) operation compared to
        // the O(1) for the ledgerCache.
        if (!interleavedLedgerStorage.ledgerExists(ledgerId)) {
            EntryKeyValue kv = memTable.getLastEntry(ledgerId);
            if (null == kv) {
                return interleavedLedgerStorage.ledgerExists(ledgerId);
            }
        }
        return true;
    }

    @Override
    public boolean entryExists(long ledgerId, long entryId) throws IOException {
        // can probably be implemented as above, but I'm not going to test it
        throw new UnsupportedOperationException("Not supported for SortedLedgerStorage");
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        return interleavedLedgerStorage.setFenced(ledgerId);
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        return interleavedLedgerStorage.isFenced(ledgerId);
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        interleavedLedgerStorage.setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return interleavedLedgerStorage.readMasterKey(ledgerId);
    }

    @Override
    public long addEntry(ByteBuf entry) throws IOException {
        long ledgerId = entry.getLong(entry.readerIndex() + 0);
        long entryId = entry.getLong(entry.readerIndex() + 8);
        long lac = entry.getLong(entry.readerIndex() + 16);

        memTable.addEntry(ledgerId, entryId, entry.nioBuffer(), this);
        interleavedLedgerStorage.ledgerCache.updateLastAddConfirmed(ledgerId, lac);
        return entryId;
    }

    /**
     * Get the last entry id for a particular ledger.
     * @param ledgerId
     * @return
     */
    private ByteBuf getLastEntryId(long ledgerId) throws IOException {
        EntryKeyValue kv = memTable.getLastEntry(ledgerId);
        if (null != kv) {
            return kv.getValueAsByteBuffer();
        }
        // If it doesn't exist in the skip list, then fallback to the ledger cache+index.
        return interleavedLedgerStorage.getEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED);
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException, BookieException {
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return getLastEntryId(ledgerId);
        }
        ByteBuf buffToRet;
        try {
            buffToRet = interleavedLedgerStorage.getEntry(ledgerId, entryId);
        } catch (Bookie.NoEntryException nee) {
            EntryKeyValue kv = memTable.getEntry(ledgerId, entryId);
            if (null == kv) {
                // The entry might have been flushed since we last checked, so query the ledger cache again.
                // If the entry truly doesn't exist, then this will throw a NoEntryException
                buffToRet = interleavedLedgerStorage.getEntry(ledgerId, entryId);
            } else {
                buffToRet = kv.getValueAsByteBuffer();
            }
        }
        // buffToRet will not be null when we reach here.
        return buffToRet;
    }

    @Override
    public long getLastAddConfirmed(long ledgerId) throws IOException {
        return interleavedLedgerStorage.getLastAddConfirmed(ledgerId);
    }

    @Override
    public boolean waitForLastAddConfirmedUpdate(long ledgerId,
                                                 long previousLAC,
                                                 Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        return interleavedLedgerStorage.waitForLastAddConfirmedUpdate(ledgerId, previousLAC, watcher);
    }

    @Override
    public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                    Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        interleavedLedgerStorage.cancelWaitForLastAddConfirmedUpdate(ledgerId, watcher);
    }

    @Override
    public void checkpoint(final Checkpoint checkpoint) throws IOException {
        long numBytesFlushed = memTable.flush(this, checkpoint);
        interleavedLedgerStorage.getEntryLogger().prepareSortedLedgerStorageCheckpoint(numBytesFlushed);
        interleavedLedgerStorage.checkpoint(checkpoint);
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        interleavedLedgerStorage.deleteLedger(ledgerId);
    }

    @Override
    public void registerLedgerDeletionListener(LedgerDeletionListener listener) {
        interleavedLedgerStorage.registerLedgerDeletionListener(listener);
    }

    @Override
    public void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        interleavedLedgerStorage.setExplicitLac(ledgerId, lac);
    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) {
        return interleavedLedgerStorage.getExplicitLac(ledgerId);
    }

    @Override
    public void process(long ledgerId, long entryId,
                        ByteBuf buffer) throws IOException {
        interleavedLedgerStorage.processEntry(ledgerId, entryId, buffer, false);
    }

    @Override
    public void flush() throws IOException {
        memTable.flush(this, Checkpoint.MAX);
        interleavedLedgerStorage.flush();
    }

    // CacheCallback functions.
    @Override
    public void onSizeLimitReached(final Checkpoint cp) throws IOException {
        LOG.info("Reached size {}", cp);
        // when size limit reached, we get the previous checkpoint from snapshot mem-table.
        // at this point, we are safer to schedule a checkpoint, since the entries added before
        // this checkpoint already written to entry logger.
        // but it would be better not to let mem-table flush to different entry log files,
        // so we roll entry log files in SortedLedgerStorage itself.
        // After that, we could make the process writing data to entry logger file not bound with checkpoint.
        // otherwise, it hurts add performance.
        //
        // The only exception for the size limitation is if a file grows to be more than hard limit 2GB,
        // we have to force rolling log, which it might cause slight performance effects
        scheduler.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info("Started flushing mem table.");
                    interleavedLedgerStorage.getEntryLogger().prepareEntryMemTableFlush();
                    memTable.flush(SortedLedgerStorage.this);
                    if (interleavedLedgerStorage.getEntryLogger().commitEntryMemTableFlush()) {
                        interleavedLedgerStorage.checkpointer.startCheckpoint(cp);
                    }
                } catch (Exception e) {
                    stateManager.transitionToReadOnlyMode();
                    LOG.error("Exception thrown while flushing skip list cache.", e);
                }
            }
        });
    }

    @Override
    public void onRotateEntryLog() {
        // override the behavior at interleaved ledger storage.
        // we don't trigger any checkpoint logic when an entry log file is rotated, because entry log file rotation
        // can happen because compaction. in a sorted ledger storage, checkpoint should happen after the data is
        // flushed to the entry log file.
    }

    BookieStateManager getStateManager(){
        return (BookieStateManager) stateManager;
    }

    public DefaultEntryLogger getEntryLogger() {
        return interleavedLedgerStorage.getEntryLogger();
    }

    @Override
    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) throws IOException {
        return interleavedLedgerStorage.getActiveLedgersInRange(firstLedgerId, lastLedgerId);
    }

    @Override
    public void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException {
        interleavedLedgerStorage.updateEntriesLocations(locations);
    }

    @Override
    public void flushEntriesLocationsIndex() throws IOException {
        interleavedLedgerStorage.flushEntriesLocationsIndex();
    }

    @Override
    public LedgerStorage getUnderlyingLedgerStorage() {
        return interleavedLedgerStorage;
    }

    @Override
    public void forceGC() {
        interleavedLedgerStorage.forceGC();
    }

    @Override
    public void forceGC(boolean forceMajor, boolean forceMinor) {
        interleavedLedgerStorage.forceGC(forceMajor, forceMinor);
    }

    @Override
    public void suspendMinorGC() {
        interleavedLedgerStorage.suspendMinorGC();
    }

    @Override
    public void suspendMajorGC() {
        interleavedLedgerStorage.suspendMajorGC();
    }

    @Override
    public void resumeMinorGC() {
        interleavedLedgerStorage.resumeMinorGC();
    }

    @Override
    public void resumeMajorGC() {
        interleavedLedgerStorage.resumeMajorGC();
    }

    @Override
    public boolean isMajorGcSuspended() {
        return interleavedLedgerStorage.isMajorGcSuspended();
    }

    @Override
    public boolean isMinorGcSuspended() {
        return interleavedLedgerStorage.isMinorGcSuspended();
    }

    @Override
    public List<DetectedInconsistency> localConsistencyCheck(Optional<RateLimiter> rateLimiter) throws IOException {
        return interleavedLedgerStorage.localConsistencyCheck(rateLimiter);
    }

    @Override
    public boolean isInForceGC() {
        return interleavedLedgerStorage.isInForceGC();
    }

    @Override
    public List<GarbageCollectionStatus> getGarbageCollectionStatus() {
        return interleavedLedgerStorage.getGarbageCollectionStatus();
    }

    @Override
    public PrimitiveIterator.OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
        PrimitiveIterator.OfLong entriesInMemtableItr = memTable.getListOfEntriesOfLedger(ledgerId);
        PrimitiveIterator.OfLong entriesFromILSItr = interleavedLedgerStorage.getListOfEntriesOfLedger(ledgerId);
        return IteratorUtility.mergePrimitiveLongIterator(entriesInMemtableItr, entriesFromILSItr);
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
