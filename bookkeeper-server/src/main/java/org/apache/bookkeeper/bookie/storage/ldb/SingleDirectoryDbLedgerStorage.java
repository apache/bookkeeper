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
package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.CompactableLedgerStorage;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.GarbageCollectionStatus;
import org.apache.bookkeeper.bookie.GarbageCollectorThread;
import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerEntryPage;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.Batch;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single directory implementation of LedgerStorage that uses RocksDB to keep the indexes for entries stored in
 * EntryLogs.
 *
 * <p>This is meant only to be used from {@link DbLedgerStorage}.
 */
public class SingleDirectoryDbLedgerStorage implements CompactableLedgerStorage {
    private final EntryLogger entryLogger;

    private final LedgerMetadataIndex ledgerIndex;
    private final EntryLocationIndex entryLocationIndex;

    private final ConcurrentLongHashMap<TransientLedgerInfo> transientLedgerInfoCache;

    private final GarbageCollectorThread gcThread;

    protected volatile WriteCacheContainer writeCacheContainer;

    // Cache where we insert entries for speculative reading
    private final ReadCache readCache;

    protected final ReentrantLock flushMutex = new ReentrantLock();

    // Executor used to for db index cleanup
    private final ScheduledExecutorService cleanupExecutor = Executors
            .newSingleThreadScheduledExecutor(new DefaultThreadFactory("db-storage-cleanup"));

    private final CopyOnWriteArrayList<LedgerDeletionListener> ledgerDeletionListeners = Lists
            .newCopyOnWriteArrayList();

    private CheckpointSource checkpointSource = CheckpointSource.DEFAULT;

    private final long readCacheMaxSize;
    private final int readAheadCacheBatchSize;
    
    private final DbLedgerStorageStats dbLedgerStorageStats;

    private static final long DEFAULT_MAX_THROTTLE_TIME_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private final long maxReadAheadBytesSize;

    public SingleDirectoryDbLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
            LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager, StatsLogger statsLogger,
            ByteBufAllocator allocator, ScheduledExecutorService gcExecutor, long writeCacheSize, long readCacheSize,
                                          int readAheadCacheBatchSize, int writeCacheNum) throws IOException {
        checkArgument(ledgerDirsManager.getAllLedgerDirs().size() == 1,
                "Db implementation only allows for one storage dir");
        checkArgument(writeCacheNum > 1, "dbStorage_writeCacheNum <2");

        String baseDir = ledgerDirsManager.getAllLedgerDirs().get(0).toString();
        log.info("Creating single directory db ledger storage on {}", baseDir);

        StatsLogger ledgerDirStatsLogger = statsLogger.scopeLabel("ledgerDir",
                ledgerDirsManager.getAllLedgerDirs().get(0).getPath());

        readCacheMaxSize = readCacheSize;
        this.readAheadCacheBatchSize = readAheadCacheBatchSize;

        // Do not attempt to perform read-ahead more than half the total size of the cache
        maxReadAheadBytesSize = readCacheMaxSize / 2;

        long maxThrottleTimeMillis = conf.getLong(DbLedgerStorage.MAX_THROTTLE_TIME_MILLIS,
                DEFAULT_MAX_THROTTLE_TIME_MILLIS);

        transientLedgerInfoCache = ConcurrentLongHashMap.<TransientLedgerInfo>newBuilder()
                .expectedItems(16 * 1024)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors() * 2)
                .build();
        ledgerIndex = new LedgerMetadataIndex(conf, KeyValueStorageRocksDB.factory, baseDir, ledgerDirStatsLogger);
        entryLocationIndex = new EntryLocationIndex(conf,
                KeyValueStorageRocksDB.factory, baseDir, ledgerDirStatsLogger);

        entryLogger = new EntryLogger(conf, ledgerDirsManager, null, statsLogger, allocator);

        this.writeCacheContainer = new WriteCacheContainer(allocator, writeCacheSize, writeCacheNum,
                TimeUnit.MILLISECONDS.toNanos(maxThrottleTimeMillis), ledgerDirStatsLogger,
                transientLedgerInfoCache, entryLocationIndex, entryLogger, ledgerIndex, flushMutex);

        readCache = new ReadCache(allocator, readCacheMaxSize);

        
        cleanupExecutor.scheduleAtFixedRate(this::cleanupStaleTransientLedgerInfo,
                TransientLedgerInfo.LEDGER_INFO_CACHING_TIME_MINUTES,
                TransientLedgerInfo.LEDGER_INFO_CACHING_TIME_MINUTES, TimeUnit.MINUTES);

        gcThread = new GarbageCollectorThread(conf, ledgerManager, ledgerDirsManager, this, statsLogger);

        dbLedgerStorageStats = new DbLedgerStorageStats(
                ledgerDirStatsLogger,
            () -> writeCacheContainer.size(),
            () -> writeCacheContainer.count(),
            () -> readCache.size(),
            () -> readCache.count()
        );
        writeCacheContainer.setDbLedgerStorageStats(dbLedgerStorageStats);

        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
    }

    @Override
    public void initialize(ServerConfiguration conf, LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager,
            LedgerDirsManager indexDirsManager, StatsLogger statsLogger,
            ByteBufAllocator allocator) throws IOException {
        /// Initialized in constructor
    }

    @Override
    public void setStateManager(StateManager stateManager) { }

    @Override
    public void setCheckpointSource(CheckpointSource checkpointSource) {
        this.checkpointSource = checkpointSource;
    }
    @Override
    public void setCheckpointer(Checkpointer checkpointer) { }

    /**
     * Evict all the ledger info object that were not used recently.
     */
    private void cleanupStaleTransientLedgerInfo() {
        transientLedgerInfoCache.removeIf((ledgerId, ledgerInfo) -> {
            boolean isStale = ledgerInfo.isStale();
            if (isStale) {
                ledgerInfo.close();
            }

            return isStale;
        });
    }

    @Override
    public void start() {
        gcThread.start();
    }

    @Override
    public void forceGC() {
        gcThread.enableForceGC();
    }

    @Override
    public boolean isInForceGC() {
        return gcThread.isInForceGC();
    }

    @Override
    public void shutdown() throws InterruptedException {
        try {
            flush();

            gcThread.shutdown();
            entryLogger.shutdown();

            cleanupExecutor.shutdown();
            cleanupExecutor.awaitTermination(1, TimeUnit.SECONDS);

            ledgerIndex.close();
            entryLocationIndex.close();

            readCache.close();
            writeCacheContainer.shutdown();
        } catch (IOException e) {
            log.error("Error closing db storage", e);
        }
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        try {
            LedgerData ledgerData = ledgerIndex.get(ledgerId);
            if (log.isDebugEnabled()) {
                log.debug("Ledger exists. ledger: {} : {}", ledgerId, ledgerData.getExists());
            }
            return ledgerData.getExists();
        } catch (Bookie.NoLedgerException nle) {
            // ledger does not exist
            return false;
        }
    }

    @Override
    public boolean entryExists(long ledgerId, long entryId) throws IOException, BookieException {
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return false;
        }

        boolean inCache = writeCacheContainer.entryExists(ledgerId,entryId)
             || readCache.hasEntry(ledgerId, entryId);

        if (inCache) {
            return true;
        }

        // Read from main storage
        long entryLocation = entryLocationIndex.getLocation(ledgerId, entryId);
        if (entryLocation != 0) {
            return true;
        }

        // Only a negative result while in limbo equates to unknown
        throwIfLimbo(ledgerId);

        return false;
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException, BookieException {
        if (log.isDebugEnabled()) {
            log.debug("isFenced. ledger: {}", ledgerId);
        }

        boolean isFenced = ledgerIndex.get(ledgerId).getFenced();

        // Only a negative result while in limbo equates to unknown
        if (!isFenced) {
            throwIfLimbo(ledgerId);
        }

        return isFenced;
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Set fenced. ledger: {}", ledgerId);
        }
        boolean changed = ledgerIndex.setFenced(ledgerId);
        if (changed) {
            // notify all the watchers if a ledger is fenced
            TransientLedgerInfo ledgerInfo = transientLedgerInfoCache.get(ledgerId);
            if (null != ledgerInfo) {
                ledgerInfo.notifyWatchers(Long.MAX_VALUE);
            }
        }
        return changed;
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Set master key. ledger: {}", ledgerId);
        }
        ledgerIndex.setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        if (log.isDebugEnabled()) {
            log.debug("Read master key. ledger: {}", ledgerId);
        }
        return ledgerIndex.get(ledgerId).getMasterKey().toByteArray();
    }

    @Override
    public long addEntry(ByteBuf entry) throws IOException, BookieException {
       return writeCacheContainer.addEntry(entry);
    }


    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException, BookieException {
        long startTime = MathUtils.nowInNano();
        try {
            ByteBuf entry = doGetEntry(ledgerId, entryId);
            recordSuccessfulEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
            return entry;
        } catch (IOException e) {
            recordFailedEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
            throw e;
        }
    }

    private ByteBuf doGetEntry(long ledgerId, long entryId) throws IOException, BookieException {
        if (log.isDebugEnabled()) {
            log.debug("Get Entry: {}@{}", ledgerId, entryId);
        }

        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return getLastEntry(ledgerId);
        }
        ByteBuf entry = writeCacheContainer.doGetEntry(ledgerId, entryId);

        if (entry != null) {
            return entry;
        }

        // Try reading from read-ahead cache
        entry = readCache.get(ledgerId, entryId);
        if (entry != null) {
            dbLedgerStorageStats.getReadCacheHitCounter().inc();
            return entry;
        }

        dbLedgerStorageStats.getReadCacheMissCounter().inc();

        // Read from main storage
        long entryLocation;
        long locationIndexStartNano = MathUtils.nowInNano();
        try {
            entryLocation = entryLocationIndex.getLocation(ledgerId, entryId);
            if (entryLocation == 0) {
                // Only a negative result while in limbo equates to unknown
                throwIfLimbo(ledgerId);

                throw new NoEntryException(ledgerId, entryId);
            }
        } finally {
            dbLedgerStorageStats.getReadFromLocationIndexTime().add(MathUtils.elapsedNanos(locationIndexStartNano));
        }

        long readEntryStartNano = MathUtils.nowInNano();
        try {
            entry = entryLogger.readEntry(ledgerId, entryId, entryLocation);
        } finally {
            dbLedgerStorageStats.getReadFromEntryLogTime().add(MathUtils.elapsedNanos(readEntryStartNano));
        }

        readCache.put(ledgerId, entryId, entry);

        // Try to read more entries
        long nextEntryLocation = entryLocation + 4 /* size header */ + entry.readableBytes();
        fillReadAheadCache(ledgerId, entryId + 1, nextEntryLocation);

        return entry;
    }

    private void fillReadAheadCache(long orginalLedgerId, long firstEntryId, long firstEntryLocation) {
        long readAheadStartNano = MathUtils.nowInNano();
        int count = 0;
        long size = 0;

        try {
            long firstEntryLogId = (firstEntryLocation >> 32);
            long currentEntryLogId = firstEntryLogId;
            long currentEntryLocation = firstEntryLocation;

            while (count < readAheadCacheBatchSize
                    && size < maxReadAheadBytesSize
                    && currentEntryLogId == firstEntryLogId) {
                ByteBuf entry = entryLogger.internalReadEntry(orginalLedgerId, firstEntryId, currentEntryLocation,
                        false /* validateEntry */);

                try {
                    long currentEntryLedgerId = entry.getLong(0);
                    long currentEntryId = entry.getLong(8);

                    if (currentEntryLedgerId != orginalLedgerId) {
                        // Found an entry belonging to a different ledger, stopping read-ahead
                        break;
                    }

                    // Insert entry in read cache
                    readCache.put(orginalLedgerId, currentEntryId, entry);

                    count++;
                    firstEntryId++;
                    size += entry.readableBytes();

                    currentEntryLocation += 4 + entry.readableBytes();
                    currentEntryLogId = currentEntryLocation >> 32;
                } finally {
                    entry.release();
                }
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Exception during read ahead for ledger: {}: e", orginalLedgerId, e);
            }
        } finally {
            dbLedgerStorageStats.getReadAheadBatchCountStats().registerSuccessfulValue(count);
            dbLedgerStorageStats.getReadAheadBatchSizeStats().registerSuccessfulValue(size);
            dbLedgerStorageStats.getReadAheadTime().add(MathUtils.elapsedNanos(readAheadStartNano));
        }
    }

    public ByteBuf getLastEntry(long ledgerId) throws IOException, BookieException {
        throwIfLimbo(ledgerId);

        ByteBuf entry = writeCacheContainer.getLastEntry(ledgerId);
        if (entry != null) {
            return entry;
        }

        // Search the last entry in storage
        long locationIndexStartNano = MathUtils.nowInNano();
        long lastEntryId = entryLocationIndex.getLastEntryInLedger(ledgerId);
        if (log.isDebugEnabled()) {
            log.debug("Found last entry for ledger {} in db: {}", ledgerId, lastEntryId);
        }

        long entryLocation = entryLocationIndex.getLocation(ledgerId, lastEntryId);
        dbLedgerStorageStats.getReadFromLocationIndexTime().add(MathUtils.elapsedNanos(locationIndexStartNano));

        long readEntryStartNano = MathUtils.nowInNano();
        ByteBuf content = entryLogger.readEntry(ledgerId, lastEntryId, entryLocation);
        dbLedgerStorageStats.getReadFromEntryLogTime().add(MathUtils.elapsedNanos(readEntryStartNano));
        return content;
    }

    @VisibleForTesting
    boolean isFlushRequired() {
        return writeCacheContainer.isFlushRequired();
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws IOException {
        writeCacheContainer.checkpoint(checkpoint);
    }


    @Override
    public void flush() throws IOException {
       writeCacheContainer.flush();
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
       writeCacheContainer.deleteLedger(ledgerId);

        for (int i = 0, size = ledgerDeletionListeners.size(); i < size; i++) {
            LedgerDeletionListener listener = ledgerDeletionListeners.get(i);
            listener.ledgerDeleted(ledgerId);
        }

        TransientLedgerInfo tli = transientLedgerInfoCache.remove(ledgerId);
        if (tli != null) {
            tli.close();
        }
    }

    @Override
    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) throws IOException {
        return ledgerIndex.getActiveLedgersInRange(firstLedgerId, lastLedgerId);
    }

    @Override
    public void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException {
        // Trigger a flush to have all the entries being compacted in the db storage
        flush();

        entryLocationIndex.updateLocations(locations);
    }

    @Override
    public EntryLogger getEntryLogger() {
        return entryLogger;
    }

    @Override
    public long getLastAddConfirmed(long ledgerId) throws IOException, BookieException {
        throwIfLimbo(ledgerId);

        TransientLedgerInfo ledgerInfo = transientLedgerInfoCache.get(ledgerId);
        long lac = null != ledgerInfo ? ledgerInfo.getLastAddConfirmed() : TransientLedgerInfo.NOT_ASSIGNED_LAC;
        if (lac == TransientLedgerInfo.NOT_ASSIGNED_LAC) {
            ByteBuf bb = getEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED);
            try {
                bb.skipBytes(2 * Long.BYTES); // skip ledger id and entry id
                lac = bb.readLong();
                lac = getOrAddLedgerInfo(ledgerId).setLastAddConfirmed(lac);
            } finally {
                bb.release();
            }
        }
        return lac;
    }

    @Override
    public boolean waitForLastAddConfirmedUpdate(long ledgerId, long previousLAC,
            Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        return getOrAddLedgerInfo(ledgerId).waitForLastAddConfirmedUpdate(previousLAC, watcher);
    }

    @Override
    public void cancelWaitForLastAddConfirmedUpdate(long ledgerId,
                                                    Watcher<LastAddConfirmedUpdateNotification> watcher)
            throws IOException {
        getOrAddLedgerInfo(ledgerId).cancelWaitForLastAddConfirmedUpdate(watcher);
    }

    @Override
    public void setExplicitLac(long ledgerId, ByteBuf lac) throws IOException {
        TransientLedgerInfo ledgerInfo = getOrAddLedgerInfo(ledgerId);
        ledgerInfo.setExplicitLac(lac);
        ledgerIndex.setExplicitLac(ledgerId, lac);
        ledgerInfo.notifyWatchers(Long.MAX_VALUE);
    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) throws IOException, BookieException {
        throwIfLimbo(ledgerId);
        if (log.isDebugEnabled()) {
            log.debug("getExplicitLac ledger {}", ledgerId);
        }
        TransientLedgerInfo ledgerInfo = getOrAddLedgerInfo(ledgerId);
        if (ledgerInfo.getExplicitLac() != null) {
            if (log.isDebugEnabled()) {
                log.debug("getExplicitLac ledger {} returned from TransientLedgerInfo", ledgerId);
            }
            return ledgerInfo.getExplicitLac();
        }
        LedgerData ledgerData = ledgerIndex.get(ledgerId);
        if (!ledgerData.hasExplicitLac()) {
            if (log.isDebugEnabled()) {
                log.debug("getExplicitLac ledger {} missing from LedgerData", ledgerId);
            }
            return null;
        }
        if (ledgerData.hasExplicitLac()) {
            if (log.isDebugEnabled()) {
                log.debug("getExplicitLac ledger {} returned from LedgerData", ledgerId);
            }
            ByteString persistedLac = ledgerData.getExplicitLac();
            ledgerInfo.setExplicitLac(Unpooled.wrappedBuffer(persistedLac.toByteArray()));
        }
        return ledgerInfo.getExplicitLac();
    }

    private TransientLedgerInfo getOrAddLedgerInfo(long ledgerId) {
        return transientLedgerInfoCache.computeIfAbsent(ledgerId, l -> {
            return new TransientLedgerInfo(l, ledgerIndex);
        });
    }

    private void updateCachedLacIfNeeded(long ledgerId, long lac) {
        TransientLedgerInfo tli = transientLedgerInfoCache.get(ledgerId);
        if (tli != null) {
            tli.setLastAddConfirmed(lac);
        }
    }

    @Override
    public void flushEntriesLocationsIndex() throws IOException {
        // No-op. Location index is already flushed in updateEntriesLocations() call
    }

    /**
     * Add an already existing ledger to the index.
     *
     * <p>This method is only used as a tool to help the migration from InterleaveLedgerStorage to DbLedgerStorage
     *
     * @param ledgerId
     *            the ledger id
     * @param pages
     *            Iterator over index pages from Indexed
     * @return the number of
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public long addLedgerToIndex(long ledgerId, boolean isFenced, byte[] masterKey,
            LedgerCache.PageEntriesIterable pages) throws Exception {
        LedgerData ledgerData = LedgerData.newBuilder().setExists(true).setFenced(isFenced)
                .setMasterKey(ByteString.copyFrom(masterKey)).build();
        ledgerIndex.set(ledgerId, ledgerData);
        MutableLong numberOfEntries = new MutableLong();

        // Iterate over all the entries pages
        Batch batch = entryLocationIndex.newBatch();
        for (LedgerCache.PageEntries page: pages) {
            try (LedgerEntryPage lep = page.getLEP()) {
                lep.getEntries((entryId, location) -> {
                    entryLocationIndex.addLocation(batch, ledgerId, entryId, location);
                    numberOfEntries.increment();
                    return true;
                });
            }
        }

        batch.flush();
        batch.close();

        return numberOfEntries.longValue();
    }

    @Override
    public void registerLedgerDeletionListener(LedgerDeletionListener listener) {
        ledgerDeletionListeners.add(listener);
    }

    public EntryLocationIndex getEntryLocationIndex() {
        return entryLocationIndex;
    }

    private void recordSuccessfulEvent(OpStatsLogger logger, long startTimeNanos) {
        logger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
    }

    private void recordFailedEvent(OpStatsLogger logger, long startTimeNanos) {
        logger.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
    }

    @Override
    public List<GarbageCollectionStatus> getGarbageCollectionStatus() {
        return Collections.singletonList(gcThread.getGarbageCollectionStatus());
    }

    /**
     * Interface which process ledger logger.
     */
    public interface LedgerLoggerProcessor {
        void process(long entryId, long entryLogId, long position);
    }

    private static final Logger log = LoggerFactory.getLogger(SingleDirectoryDbLedgerStorage.class);

    @Override
    public OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
        throw new UnsupportedOperationException(
                "getListOfEntriesOfLedger method is currently unsupported for SingleDirectoryDbLedgerStorage");
    }

    private LedgerDirsManager.LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {

            @Override
            public void diskAlmostFull(File disk) {
                if (gcThread.isForceGCAllowWhenNoSpace()) {
                    gcThread.enableForceGC();
                } else {
                    gcThread.suspendMajorGC();
                }
            }

            @Override
            public void diskFull(File disk) {
                if (gcThread.isForceGCAllowWhenNoSpace()) {
                    gcThread.enableForceGC();
                } else {
                    gcThread.suspendMajorGC();
                    gcThread.suspendMinorGC();
                }
            }

            @Override
            public void allDisksFull(boolean highPriorityWritesAllowed) {
                if (gcThread.isForceGCAllowWhenNoSpace()) {
                    gcThread.enableForceGC();
                } else {
                    gcThread.suspendMajorGC();
                    gcThread.suspendMinorGC();
                }
            }

            @Override
            public void diskWritable(File disk) {
                // we have enough space now
                if (gcThread.isForceGCAllowWhenNoSpace()) {
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
                if (gcThread.isForceGCAllowWhenNoSpace()) {
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
    public void setLimboState(long ledgerId) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("setLimboState. ledger: {}", ledgerId);
        }
        ledgerIndex.setLimbo(ledgerId);
    }

    @Override
    public boolean hasLimboState(long ledgerId) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("hasLimboState. ledger: {}", ledgerId);
        }
        return ledgerIndex.get(ledgerId).getLimbo();
    }

    @Override
    public void clearLimboState(long ledgerId) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("clearLimboState. ledger: {}", ledgerId);
        }
        ledgerIndex.clearLimbo(ledgerId);
    }

    private void throwIfLimbo(long ledgerId) throws IOException, BookieException {
        if (hasLimboState(ledgerId)) {
            if (log.isDebugEnabled()) {
                log.debug("Accessing ledger({}) in limbo state, throwing exception", ledgerId);
            }
            throw BookieException.create(BookieException.Code.DataUnknownException);
        }
    }

    /**
     * Mapping of enums to bitmaps. The bitmaps must not overlap so that we can
     * do bitwise operations on them.
     */
    private static final Map<StorageState, Integer> stateBitmaps = ImmutableMap.of(
            StorageState.NEEDS_INTEGRITY_CHECK, 0x00000001);

    @Override
    public EnumSet<StorageState> getStorageStateFlags() throws IOException {
        int flags = ledgerIndex.getStorageStateFlags();
        EnumSet<StorageState> flagsEnum = EnumSet.noneOf(StorageState.class);
        for (Map.Entry<StorageState, Integer> e : stateBitmaps.entrySet()) {
            int value = e.getValue();
            if ((flags & value) == value) {
                flagsEnum.add(e.getKey());
            }
            flags = flags & ~value;
        }
        checkState(flags == 0, "Unknown storage state flag found " + flags);
        return flagsEnum;
    }

    @Override
    public void setStorageStateFlag(StorageState flag) throws IOException {
        checkArgument(stateBitmaps.containsKey(flag), "Unsupported flag " + flag);
        int flagInt = stateBitmaps.get(flag);
        while (true) {
            int curFlags = ledgerIndex.getStorageStateFlags();
            int newFlags = curFlags | flagInt;
            if (ledgerIndex.setStorageStateFlags(curFlags, newFlags)) {
                return;
            } else {
                log.info("Conflict updating storage state flags {} -> {}, retrying",
                        curFlags, newFlags);
            }
        }
    }

    @Override
    public void clearStorageStateFlag(StorageState flag) throws IOException {
        checkArgument(stateBitmaps.containsKey(flag), "Unsupported flag " + flag);
        int flagInt = stateBitmaps.get(flag);
        while (true) {
            int curFlags = ledgerIndex.getStorageStateFlags();
            int newFlags = curFlags & ~flagInt;
            if (ledgerIndex.setStorageStateFlags(curFlags, newFlags)) {
                return;
            } else {
                log.info("Conflict updating storage state flags {} -> {}, retrying",
                        curFlags, newFlags);
            }
        }
    }
}
