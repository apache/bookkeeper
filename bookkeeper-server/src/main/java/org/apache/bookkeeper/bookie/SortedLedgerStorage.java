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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.IteratorUtility;
import org.apache.commons.lang3.StringUtils;
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
    private ServerConfiguration conf;
    private StatsLogger statsLogger;
    protected final List<InterleavedLedgerStorage> interleavedLedgerStorageList;
    private int numberOfDirs;
    private String interleavedLedgerStorageClazz = InterleavedLedgerStorage.class.getName();

    public SortedLedgerStorage() {
        this(null);
    }

    @VisibleForTesting
    protected SortedLedgerStorage(String interleavedLedgerStorageClazz) {
        interleavedLedgerStorageList = Lists.newArrayList();
        if (StringUtils.isNotBlank(interleavedLedgerStorageClazz)) {
            this.interleavedLedgerStorageClazz = interleavedLedgerStorageClazz;
        }
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
        this.numberOfDirs = ledgerDirsManager.getAllLedgerDirs().size();

        for (int i = 0; i < ledgerDirsManager.getAllLedgerDirs().size(); i++) {
            LedgerDirsManager ldm = BookieResources.createLedgerDirsManager(
                    conf, ledgerDirsManager.getDiskChecker(), NullStatsLogger.INSTANCE);
            LedgerDirsManager idm = BookieResources.createIndexDirsManager(
                    conf, indexDirsManager.getDiskChecker(), NullStatsLogger.INSTANCE, ledgerDirsManager);

            InterleavedLedgerStorage interleavedLedgerStorage = getInterleavedLedgerStorage();
            interleavedLedgerStorage.initializeWithEntryLogListener(
                    conf,
                    ledgerManager,
                    ldm,
                    idm,
                    // uses sorted ledger storage's own entry log listener
                    // since it manages entry log rotations and checkpoints.
                    this,
                    statsLogger,
                    allocator);
            interleavedLedgerStorageList.add(interleavedLedgerStorage);
        }

        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setNameFormat("SortedLedgerStorage-%d")
                .setPriority((Thread.NORM_PRIORITY + Thread.MAX_PRIORITY) / 2).build());
    }

    private InterleavedLedgerStorage getInterleavedLedgerStorage() {
        if (InterleavedLedgerStorage.class.getName().equals(this.interleavedLedgerStorageClazz)) {
            return new InterleavedLedgerStorage();
        } else {
            try {
                return ReflectionUtils.newInstance(this.interleavedLedgerStorageClazz, InterleavedLedgerStorage.class);
            } catch (Throwable t) {
                throw new RuntimeException("Failed to instantiate InterleavedLedgerStorage class : "
                        + this.interleavedLedgerStorageClazz, t);
            }
        }
    }


    @Override
    public void setStateManager(StateManager stateManager) {
        interleavedLedgerStorageList.forEach(s -> s.setStateManager(stateManager));
    }

    @Override
    public void setCheckpointSource(CheckpointSource checkpointSource) {
        interleavedLedgerStorageList.forEach(s -> s.setCheckpointSource(checkpointSource));

        if (conf.isEntryLogPerLedgerEnabled()) {
            this.memTable = new EntryMemTableWithParallelFlusher(conf, checkpointSource, statsLogger);
        } else {
            this.memTable = new EntryMemTable(conf, checkpointSource, statsLogger);
        }
    }
    @Override
    public void setCheckpointer(Checkpointer checkpointer) {
        interleavedLedgerStorageList.forEach(s -> s.setCheckpointer(checkpointer));
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
        interleavedLedgerStorageList.forEach(s -> s.start());
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
        for (LedgerStorage ls : interleavedLedgerStorageList) {
            ls.shutdown();
        }
    }

    private InterleavedLedgerStorage getLedgerStorage(long ledgerId) {
        return interleavedLedgerStorageList.get(MathUtils.signSafeMod(ledgerId, numberOfDirs));
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        // Done this way because checking the skip list is an O(logN) operation compared to
        // the O(1) for the ledgerCache.
        if (!getLedgerStorage(ledgerId).ledgerExists(ledgerId)) {
            EntryKeyValue kv = memTable.getLastEntry(ledgerId);
            if (null == kv) {
                return getLedgerStorage(ledgerId).ledgerExists(ledgerId);
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
        return getLedgerStorage(ledgerId).setFenced(ledgerId);
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        return getLedgerStorage(ledgerId).isFenced(ledgerId);
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        getLedgerStorage(ledgerId).setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return getLedgerStorage(ledgerId).readMasterKey(ledgerId);
    }

    @Override
    public long addEntry(ByteBuf entry) throws IOException {
        long ledgerId = entry.getLong(entry.readerIndex() + 0);
        long entryId = entry.getLong(entry.readerIndex() + 8);
        long lac = entry.getLong(entry.readerIndex() + 16);

        memTable.addEntry(ledgerId, entryId, entry.nioBuffer(), this);
        getLedgerStorage(ledgerId).ledgerCache.updateLastAddConfirmed(ledgerId, lac);
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
        return getLedgerStorage(ledgerId).getEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED);
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException, BookieException {
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return getLastEntryId(ledgerId);
        }
        ByteBuf buffToRet;
        try {
            buffToRet = getLedgerStorage(ledgerId).getEntry(ledgerId, entryId);
        } catch (Bookie.NoEntryException nee) {
            EntryKeyValue kv = memTable.getEntry(ledgerId, entryId);
            if (null == kv) {
                // The entry might have been flushed since we last checked, so query the ledger cache again.
                // If the entry truly doesn't exist, then this will throw a NoEntryException
                buffToRet = getLedgerStorage(ledgerId).getEntry(ledgerId, entryId);
            } else {
                buffToRet = kv.getValueAsByteBuffer();
            }
        }
        // buffToRet will not be null when we reach here.
        return buffToRet;
    }

    @Override
    public long getLastAddConfirmed(long ledgerId) throws IOException {
        return getLedgerStorage(ledgerId).getLastAddConfirmed(ledgerId);
    }

    @Override
    public boolean waitForLastAddConfirmedUpdate(long ledgerId,
                                                 long previousLAC,
                                                 Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        return getLedgerStorage(ledgerId).waitForLastAddConfirmedUpdate(ledgerId, previousLAC, watcher);
    }

    @Override
    public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                    Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        getLedgerStorage(ledgerId).cancelWaitForLastAddConfirmedUpdate(ledgerId, watcher);
    }

    @Override
    public void checkpoint(final Checkpoint checkpoint) throws IOException {
        long numBytesFlushed = memTable.flush(this, checkpoint);
        for (InterleavedLedgerStorage s : interleavedLedgerStorageList) {
            s.getEntryLogger().prepareSortedLedgerStorageCheckpoint(numBytesFlushed);
            s.checkpoint(checkpoint);
        }
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        getLedgerStorage(ledgerId).deleteLedger(ledgerId);
    }

    @Override
    public void registerLedgerDeletionListener(LedgerDeletionListener listener) {
        interleavedLedgerStorageList.forEach(s -> s.registerLedgerDeletionListener(listener));
    }

    @Override
    public void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        getLedgerStorage(ledgerId).setExplicitLac(ledgerId, lac);
    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) {
        return getLedgerStorage(ledgerId).getExplicitLac(ledgerId);
    }

    @Override
    public void process(long ledgerId, long entryId,
                        ByteBuf buffer) throws IOException {
        getLedgerStorage(ledgerId).processEntry(ledgerId, entryId, buffer, false);
    }

    @Override
    public void flush() throws IOException {
        memTable.flush(this, Checkpoint.MAX);
        for (LedgerStorage ls : interleavedLedgerStorageList) {
            ls.flush();
        }
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
                for (InterleavedLedgerStorage s : interleavedLedgerStorageList) {
                    try {
                        LOG.info("Started flushing mem table.");
                        s.getEntryLogger().prepareEntryMemTableFlush();
                        memTable.flush(SortedLedgerStorage.this);

                        if (s.getEntryLogger().commitEntryMemTableFlush()) {
                            s.checkpointer.startCheckpoint(cp);
                        }
                    } catch (Exception e) {
                        s.getStateManager().transitionToReadOnlyMode();
                        LOG.error("Exception thrown while flushing skip list cache.", e);
                    }
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

    public List<DefaultEntryLogger> getEntryLogger() {
        List<DefaultEntryLogger> listIt = new ArrayList<>(numberOfDirs);
        for (InterleavedLedgerStorage ls : interleavedLedgerStorageList) {
            listIt.add(ls.getEntryLogger());
        }
        return listIt;
    }

    @Override
    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) throws IOException {
        List<Iterable<Long>> listIt = new ArrayList<>(numberOfDirs);
        for (InterleavedLedgerStorage ls : interleavedLedgerStorageList) {
            listIt.add(ls.getActiveLedgersInRange(firstLedgerId, lastLedgerId));
        }
        return Iterables.concat(listIt);
    }

    @Override
    public void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException {
        for (InterleavedLedgerStorage s : interleavedLedgerStorageList) {
            s.updateEntriesLocations(locations);
        }
    }

    @Override
    public void flushEntriesLocationsIndex() throws IOException {
        for (InterleavedLedgerStorage s : interleavedLedgerStorageList) {
            s.flushEntriesLocationsIndex();
        }
    }

    @Override
    public LedgerStorage getUnderlyingLedgerStorage() {
        return interleavedLedgerStorageList.get(0);
    }

    @Override
    public void forceGC() {
        interleavedLedgerStorageList.forEach(s -> s.forceGC());
    }

    @Override
    public void forceGC(Boolean forceMajor, Boolean forceMinor) {
        interleavedLedgerStorageList.forEach(s -> s.forceGC(forceMajor, forceMinor));
    }

    @Override
    public List<DetectedInconsistency> localConsistencyCheck(Optional<RateLimiter> rateLimiter) throws IOException {
        List<DetectedInconsistency> listIt = new ArrayList<>(numberOfDirs);
        for (InterleavedLedgerStorage ls : interleavedLedgerStorageList) {
            listIt.addAll(ls.localConsistencyCheck(rateLimiter));
        }
        return listIt;
    }

    @Override
    public boolean isInForceGC() {
        for (InterleavedLedgerStorage s : interleavedLedgerStorageList) {
            if (!s.isInForceGC()){
                return false;
            }
        }
        return true;
    }

    @Override
    public List<GarbageCollectionStatus> getGarbageCollectionStatus() {
        List<GarbageCollectionStatus> listIt = new ArrayList<>(numberOfDirs);
        for (InterleavedLedgerStorage ls : interleavedLedgerStorageList) {
            listIt.addAll(ls.getGarbageCollectionStatus());
        }
        return listIt;
    }

    @Override
    public PrimitiveIterator.OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
        PrimitiveIterator.OfLong entriesInMemtableItr = memTable.getListOfEntriesOfLedger(ledgerId);
        PrimitiveIterator.OfLong entriesFromILSItr = getLedgerStorage(ledgerId).getListOfEntriesOfLedger(ledgerId);
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
