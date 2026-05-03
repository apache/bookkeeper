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
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import lombok.CustomLog;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.OperationRejectedException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.CompactableLedgerStorage;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.bookie.GarbageCollectionStatus;
import org.apache.bookkeeper.bookie.GarbageCollectorThread;
import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.bookie.LedgerCache;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerEntryPage;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.Batch;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.ThreadRegistry;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableLong;

/**
 * Single directory implementation of LedgerStorage that uses RocksDB to keep the indexes for entries stored in
 * EntryLogs.
 *
 * <p>This is meant only to be used from {@link DbLedgerStorage}.
 */
@CustomLog
public class SingleDirectoryDbLedgerStorage implements CompactableLedgerStorage {
    private final EntryLogger entryLogger;

    private final LedgerMetadataIndex ledgerIndex;
    private final EntryLocationIndex entryLocationIndex;

    private final ConcurrentLongHashMap<TransientLedgerInfo> transientLedgerInfoCache;

    private final GarbageCollectorThread gcThread;

    // Write cache where all new entries are inserted into
    protected volatile WriteCache writeCache;

    // Write cache that is used to swap with writeCache during flushes
    protected volatile WriteCache writeCacheBeingFlushed;

    // Cache where we insert entries for speculative reading
    private final ReadCache readCache;

    private final StampedLock writeCacheRotationLock = new StampedLock();

    protected final ReentrantLock flushMutex = new ReentrantLock();

    protected final AtomicBoolean hasFlushBeenTriggered = new AtomicBoolean(false);
    private final AtomicBoolean isFlushOngoing = new AtomicBoolean(false);

    private static String dbStoragerExecutorName = "db-storage";
    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            new DefaultThreadFactory(dbStoragerExecutorName) {
                @Override
                protected Thread newThread(Runnable r, String name) {
                    return super.newThread(ThreadRegistry.registerThread(r, dbStoragerExecutorName), name);
                }
            });

    // Executor used to for db index cleanup
    private final ScheduledExecutorService cleanupExecutor = Executors
            .newSingleThreadScheduledExecutor(new DefaultThreadFactory("db-storage-cleanup"));

    private final CopyOnWriteArrayList<LedgerDeletionListener> ledgerDeletionListeners = Lists
            .newCopyOnWriteArrayList();

    private CheckpointSource checkpointSource = CheckpointSource.DEFAULT;
    private Checkpoint lastCheckpoint = Checkpoint.MIN;

    private final long writeCacheMaxSize;
    private final long readCacheMaxSize;
    private final int readAheadCacheBatchSize;
    private final long readAheadCacheBatchBytesSize;

    private final long maxThrottleTimeNanos;

    private final DbLedgerStorageStats dbLedgerStorageStats;

    private static final long DEFAULT_MAX_THROTTLE_TIME_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private final long maxReadAheadBytesSize;

    private final Counter flushExecutorTime;
    private final boolean singleLedgerDirs;
    private final String ledgerBaseDir;
    private final String indexBaseDir;

    public SingleDirectoryDbLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
                                          LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager,
                                          EntryLogger entryLogger, StatsLogger statsLogger, ByteBufAllocator allocator,
                                          long writeCacheSize, long readCacheSize, int readAheadCacheBatchSize,
                                          long readAheadCacheBatchBytesSize)
            throws IOException {
        checkArgument(ledgerDirsManager.getAllLedgerDirs().size() == 1,
                "Db implementation only allows for one storage dir");
        ledgerBaseDir = ledgerDirsManager.getAllLedgerDirs().get(0).getPath();
        // indexBaseDir default use ledgerBaseDir
        String indexBaseDir = ledgerBaseDir;
        if (CollectionUtils.isEmpty(indexDirsManager.getAllLedgerDirs())
                || ledgerBaseDir.equals(indexDirsManager.getAllLedgerDirs().get(0).getPath())) {
            log.info().attr("indexBaseDir", indexBaseDir)
                    .log("indexDir is equals ledgerBaseDir, creating single directory db ledger storage");
        } else {
            // if indexDir is specified, set new value
            indexBaseDir = indexDirsManager.getAllLedgerDirs().get(0).getPath();
            log.info().attr("indexBaseDir", indexBaseDir)
                    .log("indexDir is specified a separate dir, creating single directory db ledger storage");
        }
        this.indexBaseDir = indexBaseDir;

        StatsLogger ledgerIndexDirStatsLogger = statsLogger
                .scopeLabel("ledgerDir", ledgerBaseDir)
                .scopeLabel("indexDir", indexBaseDir);

        this.writeCacheMaxSize = writeCacheSize;
        this.writeCache = new WriteCache(allocator, writeCacheMaxSize / 2);
        this.writeCacheBeingFlushed = new WriteCache(allocator, writeCacheMaxSize / 2);
        this.singleLedgerDirs = conf.getLedgerDirs().length == 1;

        readCacheMaxSize = readCacheSize;
        this.readAheadCacheBatchSize = readAheadCacheBatchSize;
        this.readAheadCacheBatchBytesSize = readAheadCacheBatchBytesSize;

        // Do not attempt to perform read-ahead more than half the total size of the cache
        maxReadAheadBytesSize = readCacheMaxSize / 2;

        long maxThrottleTimeMillis = conf.getLong(DbLedgerStorage.MAX_THROTTLE_TIME_MILLIS,
                DEFAULT_MAX_THROTTLE_TIME_MILLIS);
        maxThrottleTimeNanos = TimeUnit.MILLISECONDS.toNanos(maxThrottleTimeMillis);

        readCache = new ReadCache(allocator, readCacheMaxSize);

        ledgerIndex = new LedgerMetadataIndex(conf,
                KeyValueStorageRocksDB.factory, indexBaseDir, ledgerIndexDirStatsLogger);
        entryLocationIndex = new EntryLocationIndex(conf,
                KeyValueStorageRocksDB.factory, indexBaseDir, ledgerIndexDirStatsLogger);

        transientLedgerInfoCache = ConcurrentLongHashMap.<TransientLedgerInfo>newBuilder()
                .expectedItems(16 * 1024)
                .concurrencyLevel(Runtime.getRuntime().availableProcessors() * 2)
                .build();
        cleanupExecutor.scheduleAtFixedRate(this::cleanupStaleTransientLedgerInfo,
                TransientLedgerInfo.LEDGER_INFO_CACHING_TIME_MINUTES,
                TransientLedgerInfo.LEDGER_INFO_CACHING_TIME_MINUTES, TimeUnit.MINUTES);

        this.entryLogger = entryLogger;
        gcThread = new GarbageCollectorThread(conf,
                ledgerManager, ledgerDirsManager, this, entryLogger, ledgerIndexDirStatsLogger);

        dbLedgerStorageStats = new DbLedgerStorageStats(
            ledgerIndexDirStatsLogger,
            () -> writeCache.size() + writeCacheBeingFlushed.size(),
            () -> writeCache.count() + writeCacheBeingFlushed.count(),
            () -> readCache.size(),
            () -> readCache.count()
        );

        flushExecutorTime = ledgerIndexDirStatsLogger.getThreadScopedCounter("db-storage-thread-time");

        executor.submit(() -> {
            // ensure the metric gets registered on start-up as this thread only executes
            // when the write cache is full which may not happen or not for a long time
            flushExecutorTime.addLatency(0, TimeUnit.NANOSECONDS);
        });

        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener(ledgerBaseDir));
        if (!ledgerBaseDir.equals(indexBaseDir)) {
            indexDirsManager.addLedgerDirsListener(getLedgerDirsListener(indexBaseDir));
        }
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
    public void entryLocationCompact() {
        if (entryLocationIndex.isCompacting()) {
            // RocksDB already running compact.
            log.info().attr("directory", entryLocationIndex.getEntryLocationDBPath())
                    .log("Compacting directory, skipping this entryLocationCompaction this time");
            return;
        }
        cleanupExecutor.execute(() -> {
            // There can only be one single cleanup task running because the cleanupExecutor
            // is single-threaded
            try {
                log.info("Trigger entry location index RocksDB compact.");
                entryLocationIndex.compact();
            } catch (Throwable t) {
                log.warn().exception(t).log("Failed to trigger entry location index RocksDB compact");
            }
        });
    }

    @Override
    public boolean isEntryLocationCompacting() {
        return entryLocationIndex.isCompacting();
    }

    @Override
    public List<String> getEntryLocationDBPath() {
        return Lists.newArrayList(entryLocationIndex.getEntryLocationDBPath());
    }

    @Override
    public void shutdown() throws InterruptedException {
        try {
            flush();

            gcThread.shutdown();
            entryLogger.close();

            cleanupExecutor.shutdown();
            cleanupExecutor.awaitTermination(1, TimeUnit.SECONDS);

            ledgerIndex.close();
            entryLocationIndex.close();

            writeCache.close();
            writeCacheBeingFlushed.close();
            readCache.close();
            executor.shutdown();

        } catch (IOException e) {
            log.error().exception(e).log("Error closing db storage");
        }
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        try {
            LedgerData ledgerData = ledgerIndex.get(ledgerId);
            log.debug()
                    .attr("ledgerId", ledgerId)
                    .attr("exists", () -> ledgerData.getExists())
                    .log("Ledger exists");
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

        // We need to try to read from both write caches, since recent entries could be found in either of the two. The
        // write caches are already thread safe on their own, here we just need to make sure we get references to both
        // of them. Using an optimistic lock since the read lock is always free, unless we're swapping the caches.
        long stamp = writeCacheRotationLock.tryOptimisticRead();
        WriteCache localWriteCache = writeCache;
        WriteCache localWriteCacheBeingFlushed = writeCacheBeingFlushed;
        if (!writeCacheRotationLock.validate(stamp)) {
            // Fallback to regular read lock approach
            stamp = writeCacheRotationLock.readLock();
            try {
                localWriteCache = writeCache;
                localWriteCacheBeingFlushed = writeCacheBeingFlushed;
            } finally {
                writeCacheRotationLock.unlockRead(stamp);
            }
        }

        boolean inCache = localWriteCache.hasEntry(ledgerId, entryId)
             || localWriteCacheBeingFlushed.hasEntry(ledgerId, entryId)
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
        boolean isFenced = ledgerIndex.get(ledgerId).getFenced();

        log.debug()
                .attr("ledgerId", ledgerId)
                .attr("isFenced", isFenced)
                .log("isFenced check");

        // Only a negative result while in limbo equates to unknown
        if (!isFenced) {
            throwIfLimbo(ledgerId);
        }

        return isFenced;
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        log.debug().attr("ledgerId", ledgerId).log("Set fenced");
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
        log.debug().attr("ledgerId", ledgerId).log("Set master key");
        ledgerIndex.setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        log.debug().attr("ledgerId", ledgerId).log("Read master key");
        return ledgerIndex.get(ledgerId).getMasterKey().toByteArray();
    }

    @Override
    public long addEntry(ByteBuf entry) throws IOException, BookieException {
        long startTime = MathUtils.nowInNano();

        long ledgerId = entry.getLong(entry.readerIndex());
        long entryId = entry.getLong(entry.readerIndex() + 8);
        long lac = entry.getLong(entry.readerIndex() + 16);

        log.debug()
                .attr("ledgerId", ledgerId)
                .attr("entryId", entryId)
                .attr("lac", lac)
                .log("Add entry");

        // First we try to do an optimistic locking to get access to the current write cache.
        // This is based on the fact that the write cache is only being rotated (swapped) every 1 minute. During the
        // rest of the time, we can have multiple thread using the optimistic lock here without interfering.
        long stamp = writeCacheRotationLock.tryOptimisticRead();
        boolean inserted = false;

        // If the stamp is 0, the lock was exclusively acquired, validation will fail, and we can skip this put.
        if (stamp != 0) {
            inserted = writeCache.put(ledgerId, entryId, entry);
        }

        if (stamp == 0 || !writeCacheRotationLock.validate(stamp)) {
            // The write cache was rotated while we were inserting. We need to acquire the proper read lock and repeat
            // the operation because we might have inserted in a write cache that was already being flushed and cleared,
            // without being sure about this last entry being flushed or not.
            stamp = writeCacheRotationLock.readLock();
            try {
                inserted = writeCache.put(ledgerId, entryId, entry);
            } finally {
                writeCacheRotationLock.unlockRead(stamp);
            }
        }

        if (!inserted) {
            triggerFlushAndAddEntry(ledgerId, entryId, entry);
        }

        // after successfully insert the entry, update LAC and notify the watchers
        updateCachedLacIfNeeded(ledgerId, lac);

        recordSuccessfulEvent(dbLedgerStorageStats.getAddEntryStats(), startTime);
        return entryId;
    }

    private void triggerFlushAndAddEntry(long ledgerId, long entryId, ByteBuf entry)
            throws IOException, BookieException {
        long throttledStartTime = MathUtils.nowInNano();
        dbLedgerStorageStats.getThrottledWriteRequests().inc();
        long absoluteTimeoutNanos = System.nanoTime() + maxThrottleTimeNanos;

        while (System.nanoTime() < absoluteTimeoutNanos) {
            // Write cache is full, we need to trigger a flush so that it gets rotated
            // If the flush has already been triggered or flush has already switched the
            // cache, we don't need to trigger another flush
            if (!isFlushOngoing.get() && hasFlushBeenTriggered.compareAndSet(false, true)) {
                // Trigger an early flush in background
                log.info("Write cache is full, triggering flush");
                executor.execute(() -> {
                        long startTime = System.nanoTime();
                        try {
                            flush();
                        } catch (IOException e) {
                            log.error().exception(e).log("Error during flush");
                        } finally {
                            flushExecutorTime.addLatency(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                        }
                    });
            }

            long stamp = writeCacheRotationLock.readLock();
            try {
                if (writeCache.put(ledgerId, entryId, entry)) {
                    // We succeeded in putting the entry in write cache in the
                    recordSuccessfulEvent(dbLedgerStorageStats.getThrottledWriteStats(), throttledStartTime);
                    return;
                }
            } finally {
                writeCacheRotationLock.unlockRead(stamp);
            }

            // Wait some time and try again
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted when adding entry " + ledgerId + "@" + entryId);
            }
        }

        // Timeout expired and we weren't able to insert in write cache
        dbLedgerStorageStats.getRejectedWriteRequests().inc();
        recordFailedEvent(dbLedgerStorageStats.getThrottledWriteStats(), throttledStartTime);
        throw new OperationRejectedException();
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException, BookieException {
        return getEntry(ledgerId, entryId, false);
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId, boolean noReadAhead) throws IOException, BookieException {
        long startTime = MathUtils.nowInNano();
        try {
            ByteBuf entry = doGetEntry(ledgerId, entryId, noReadAhead);
            recordSuccessfulEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
            return entry;
        } catch (IOException e) {
            recordFailedEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
            throw e;
        }
    }

    private ByteBuf doGetEntry(long ledgerId, long entryId, boolean noReadAhead) throws IOException, BookieException {
        log.debug()
                .attr("ledgerId", ledgerId)
                .attr("entryId", entryId)
                .log("Get Entry");

        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return getLastEntry(ledgerId);
        }

        // We need to try to read from both write caches, since recent entries could be found in either of the two. The
        // write caches are already thread safe on their own, here we just need to make sure we get references to both
        // of them. Using an optimistic lock since the read lock is always free, unless we're swapping the caches.
        long stamp = writeCacheRotationLock.tryOptimisticRead();
        WriteCache localWriteCache = writeCache;
        WriteCache localWriteCacheBeingFlushed = writeCacheBeingFlushed;
        if (!writeCacheRotationLock.validate(stamp)) {
            // Fallback to regular read lock approach
            stamp = writeCacheRotationLock.readLock();
            try {
                localWriteCache = writeCache;
                localWriteCacheBeingFlushed = writeCacheBeingFlushed;
            } finally {
                writeCacheRotationLock.unlockRead(stamp);
            }
        }

        // First try to read from the write cache of recent entries
        ByteBuf entry = localWriteCache.get(ledgerId, entryId);
        if (entry != null) {
            dbLedgerStorageStats.getWriteCacheHitCounter().inc();
            return entry;
        }

        // If there's a flush going on, the entry might be in the flush buffer
        entry = localWriteCacheBeingFlushed.get(ledgerId, entryId);
        if (entry != null) {
            dbLedgerStorageStats.getWriteCacheHitCounter().inc();
            return entry;
        }

        dbLedgerStorageStats.getWriteCacheMissCounter().inc();

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
            dbLedgerStorageStats.getReadFromLocationIndexTime().addLatency(
                    MathUtils.elapsedNanos(locationIndexStartNano), TimeUnit.NANOSECONDS);
        }

        long readEntryStartNano = MathUtils.nowInNano();
        try {
            entry = entryLogger.readEntry(ledgerId, entryId, entryLocation);
        } finally {
            dbLedgerStorageStats.getReadFromEntryLogTime().addLatency(
                    MathUtils.elapsedNanos(readEntryStartNano), TimeUnit.NANOSECONDS);
        }

        readCache.put(ledgerId, entryId, entry);

        // Try to read more entries
        if (!noReadAhead) {
            long nextEntryLocation = entryLocation + 4 /* size header */ + entry.readableBytes();
            fillReadAheadCache(ledgerId, entryId + 1, nextEntryLocation);
        }

        return entry;
    }

    private void fillReadAheadCache(long originalLedgerId, long firstEntryId, long firstEntryLocation) {
        long readAheadStartNano = MathUtils.nowInNano();
        int count = 0;
        long size = 0;

        try {
            long firstEntryLogId = (firstEntryLocation >> 32);
            long currentEntryLogId = firstEntryLogId;
            long currentEntryLocation = firstEntryLocation;

            while (chargeReadAheadCache(count, size) && currentEntryLogId == firstEntryLogId) {
                ByteBuf entry = entryLogger.readEntry(originalLedgerId,
                        firstEntryId, currentEntryLocation);

                try {
                    long currentEntryLedgerId = entry.getLong(0);
                    long currentEntryId = entry.getLong(8);

                    if (currentEntryLedgerId != originalLedgerId) {
                        // Found an entry belonging to a different ledger, stopping read-ahead
                        break;
                    }

                    // Insert entry in read cache
                    readCache.put(originalLedgerId, currentEntryId, entry);

                    count++;
                    firstEntryId++;
                    size += entry.readableBytes();

                    currentEntryLocation += 4 + entry.readableBytes();
                    currentEntryLogId = currentEntryLocation >> 32;
                } finally {
                    ReferenceCountUtil.release(entry);
                }
            }
        } catch (Exception e) {
            log.debug()
                    .attr("ledgerId", originalLedgerId)
                    .exception(e)
                    .log("Exception during read ahead for ledger");
        } finally {
            dbLedgerStorageStats.getReadAheadBatchCountStats().registerSuccessfulValue(count);
            dbLedgerStorageStats.getReadAheadBatchSizeStats().registerSuccessfulValue(size);
            dbLedgerStorageStats.getReadAheadTime().addLatency(
                    MathUtils.elapsedNanos(readAheadStartNano), TimeUnit.NANOSECONDS);
        }
    }

    protected boolean chargeReadAheadCache(int currentReadAheadCount, long currentReadAheadBytes) {
        // compatible with old logic
        boolean chargeSizeCondition = currentReadAheadCount < readAheadCacheBatchSize
                && currentReadAheadBytes < maxReadAheadBytesSize;
        if (chargeSizeCondition && readAheadCacheBatchBytesSize > 0) {
            // exact limits limit the size and count for each batch
            chargeSizeCondition = currentReadAheadBytes < readAheadCacheBatchBytesSize;
        }
        return chargeSizeCondition;
    }

    public ByteBuf getLastEntry(long ledgerId) throws IOException, BookieException {
        throwIfLimbo(ledgerId);

        long stamp = writeCacheRotationLock.readLock();
        try {
            // First try to read from the write cache of recent entries
            ByteBuf entry = writeCache.getLastEntry(ledgerId);
            if (entry != null) {
                final ByteBuf found = entry;
                log.debug(e -> e.attr("ledgerId", ledgerId)
                        .attr("foundLedgerId", found.getLong(0))
                        .attr("entryId", found.getLong(8))
                        .log("Found last entry for ledger in write cache"));

                dbLedgerStorageStats.getWriteCacheHitCounter().inc();
                return entry;
            }

            // If there's a flush going on, the entry might be in the flush buffer
            entry = writeCacheBeingFlushed.getLastEntry(ledgerId);
            if (entry != null) {
                final ByteBuf found = entry;
                log.debug()
                        .attr("ledgerId", ledgerId)
                        .attr("entryId", () -> found.getLong(8))
                        .log("Found last entry for ledger in write cache being flushed");

                dbLedgerStorageStats.getWriteCacheHitCounter().inc();
                return entry;
            }
        } finally {
            writeCacheRotationLock.unlockRead(stamp);
        }

        dbLedgerStorageStats.getWriteCacheMissCounter().inc();

        // Search the last entry in storage
        long locationIndexStartNano = MathUtils.nowInNano();
        long lastEntryId = entryLocationIndex.getLastEntryInLedger(ledgerId);
        log.debug()
                .attr("ledgerId", ledgerId)
                .attr("lastEntryId", lastEntryId)
                .log("Found last entry for ledger in db");

        long entryLocation = entryLocationIndex.getLocation(ledgerId, lastEntryId);
        dbLedgerStorageStats.getReadFromLocationIndexTime().addLatency(
                MathUtils.elapsedNanos(locationIndexStartNano), TimeUnit.NANOSECONDS);

        long readEntryStartNano = MathUtils.nowInNano();
        ByteBuf content = entryLogger.readEntry(ledgerId, lastEntryId, entryLocation);
        dbLedgerStorageStats.getReadFromEntryLogTime().addLatency(
                MathUtils.elapsedNanos(readEntryStartNano), TimeUnit.NANOSECONDS);
        return content;
    }

    @VisibleForTesting
    boolean isFlushRequired() {
        long stamp = writeCacheRotationLock.readLock();
        try {
            return !writeCache.isEmpty();
        } finally {
            writeCacheRotationLock.unlockRead(stamp);
        }
    }

    @Override
    public void checkpoint(Checkpoint checkpoint) throws IOException {
        Checkpoint thisCheckpoint = checkpointSource.newCheckpoint();
        if (lastCheckpoint.compareTo(checkpoint) > 0) {
            return;
        }

        // Only a single flush operation can happen at a time
        flushMutex.lock();
        long startTime = -1;
        try {
            startTime = MathUtils.nowInNano();
        } catch (Throwable e) {
            // Fix spotbugs warning. Should never happen
            flushMutex.unlock();
            throw new IOException(e);
        }

        try {
            if (writeCache.isEmpty()) {
                return;
            }
            // Swap the write cache so that writes can continue to happen while the flush is
            // ongoing
            swapWriteCache();

            long sizeToFlush = writeCacheBeingFlushed.size();
            log.debug(e -> e.attr("count", writeCacheBeingFlushed.count())
                    .attr("sizeMb", sizeToFlush / 1024.0 / 1024)
                    .log("Flushing entries"));

            // Write all the pending entries into the entry logger and collect the offset
            // position for each entry

            try (Batch batch = entryLocationIndex.newBatch()) {
                writeCacheBeingFlushed.forEach((ledgerId, entryId, entry) -> {
                    long location = entryLogger.addEntry(ledgerId, entry);
                    entryLocationIndex.addLocation(batch, ledgerId, entryId, location);
                });

                long entryLoggerStart = MathUtils.nowInNano();
                entryLogger.flush();
                recordSuccessfulEvent(dbLedgerStorageStats.getFlushEntryLogStats(), entryLoggerStart);

                long batchFlushStartTime = MathUtils.nowInNano();
                batch.flush();

                recordSuccessfulEvent(dbLedgerStorageStats.getFlushLocationIndexStats(), batchFlushStartTime);
                log.debug().attr("durationSeconds",
                        () -> MathUtils.elapsedNanos(batchFlushStartTime) / (double) TimeUnit.SECONDS.toNanos(1))
                        .log("DB batch flushed");
            }

            long ledgerIndexStartTime = MathUtils.nowInNano();
            ledgerIndex.flush();
            recordSuccessfulEvent(dbLedgerStorageStats.getFlushLedgerIndexStats(), ledgerIndexStartTime);

            lastCheckpoint = thisCheckpoint;

            // Discard all the entry from the write cache, since they're now persisted
            writeCacheBeingFlushed.clear();

            double flushTimeSeconds = MathUtils.elapsedNanos(startTime) / (double) TimeUnit.SECONDS.toNanos(1);
            double flushThroughput = sizeToFlush / 1024.0 / 1024.0 / flushTimeSeconds;

            log.debug()
                    .attr("durationSeconds", flushTimeSeconds)
                    .attr("throughputMBs", flushThroughput)
                    .log("Flushing done");

            recordSuccessfulEvent(dbLedgerStorageStats.getFlushStats(), startTime);
            dbLedgerStorageStats.getFlushSizeStats().registerSuccessfulValue(sizeToFlush);
        } catch (IOException e) {
            recordFailedEvent(dbLedgerStorageStats.getFlushStats(), startTime);
            // Leave IOException as it is
            throw e;
        } finally {
            try {
                cleanupExecutor.execute(() -> {
                    // There can only be one single cleanup task running because the cleanupExecutor
                    // is single-threaded
                    try {
                        log.debug("Removing deleted ledgers from db indexes");

                        entryLocationIndex.removeOffsetFromDeletedLedgers();
                        ledgerIndex.removeDeletedLedgers();
                    } catch (Throwable t) {
                        log.warn().exception(t).log("Failed to cleanup db indexes");
                    }
                });

                isFlushOngoing.set(false);
            } finally {
                flushMutex.unlock();
            }
        }
    }

    /**
     * Swap the current write cache with the replacement cache.
     */
    private void swapWriteCache() {
        long stamp = writeCacheRotationLock.writeLock();
        try {
            // First, swap the current write-cache map with an empty one so that writes will
            // go on unaffected. Only a single flush is happening at the same time
            WriteCache tmp = writeCacheBeingFlushed;
            writeCacheBeingFlushed = writeCache;
            writeCache = tmp;

            // Set to true before updating hasFlushBeenTriggered to false.
            isFlushOngoing.set(true);
            // since the cache is switched, we can allow flush to be triggered
            hasFlushBeenTriggered.set(false);
        } finally {
            writeCacheRotationLock.unlockWrite(stamp);
        }
    }

    @Override
    public void flush() throws IOException {
        Checkpoint cp = checkpointSource.newCheckpoint();
        checkpoint(cp);
        if (singleLedgerDirs) {
            checkpointSource.checkpointComplete(cp, true);
        }
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        log.debug().attr("ledgerId", ledgerId).log("Deleting ledger");

        // Delete entries from this ledger that are still in the write cache
        long stamp = writeCacheRotationLock.readLock();
        try {
            writeCache.deleteLedger(ledgerId);
        } finally {
            writeCacheRotationLock.unlockRead(stamp);
        }

        entryLocationIndex.delete(ledgerId);
        ledgerIndex.delete(ledgerId);

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
        // Before updating the DB with the new location for the compacted entries, we need to
        // make sure that there is no ongoing flush() operation.
        // If there were a flush, we could have the following situation, which is highly
        // unlikely though possible:
        // 1. Flush operation has written the write-cache content into entry-log files
        // 2. The DB location index is not yet updated
        // 3. Compaction is triggered and starts compacting some of the recent files
        // 4. Compaction will write the "new location" into the DB
        // 5. The pending flush() will overwrite the DB with the "old location", pointing
        //    to a file that no longer exists
        //
        // To avoid this race condition, we need that all the entries that are potentially
        // included in the compaction round to have all the indexes already flushed into
        // the DB.
        // The easiest lightweight way to achieve this is to wait for any pending
        // flush operation to be completed before updating the index with the compacted
        // entries, by blocking on the flushMutex.
        flushMutex.lock();
        flushMutex.unlock();

        // We don't need to keep the flush mutex locked here while updating the DB.
        // It's fine to have a concurrent flush operation at this point, because we
        // know that none of the entries being flushed was included in the compaction
        // round that we are dealing with.
        entryLocationIndex.updateLocations(locations);
    }

    @VisibleForTesting
    EntryLogger getEntryLogger() {
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
                ReferenceCountUtil.release(bb);
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
        log.debug().attr("ledgerId", ledgerId).log("getExplicitLac");
        TransientLedgerInfo ledgerInfo = getOrAddLedgerInfo(ledgerId);
        if (ledgerInfo.getExplicitLac() != null) {
            log.debug().attr("ledgerId", ledgerId).log("getExplicitLac returned from TransientLedgerInfo");
            return ledgerInfo.getExplicitLac();
        }
        LedgerData ledgerData = ledgerIndex.get(ledgerId);
        if (!ledgerData.hasExplicitLac()) {
            log.debug().attr("ledgerId", ledgerId).log("getExplicitLac missing from LedgerData");
            return null;
        }
        log.debug().attr("ledgerId", ledgerId).log("getExplicitLac returned from LedgerData");
        ByteString persistedLac = ledgerData.getExplicitLac();
        ledgerInfo.setExplicitLac(Unpooled.wrappedBuffer(persistedLac.toByteArray()));
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
        try (Batch batch = entryLocationIndex.newBatch()) {
            for (LedgerCache.PageEntries page : pages) {
                try (LedgerEntryPage lep = page.getLEP()) {
                    lep.getEntries((entryId, location) -> {
                        entryLocationIndex.addLocation(batch, ledgerId, entryId, location);
                        numberOfEntries.increment();
                        return true;
                    });
                }
            }

            ledgerIndex.flush();
            batch.flush();
        }

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

    @Override
    public OfLong getListOfEntriesOfLedger(long ledgerId) throws IOException {
        throw new UnsupportedOperationException(
                "getListOfEntriesOfLedger method is currently unsupported for SingleDirectoryDbLedgerStorage");
    }

    private LedgerDirsManager.LedgerDirsListener getLedgerDirsListener(String diskPath) {
        return new LedgerDirsListener() {
            private final String currentFilePath = diskPath;

            private boolean isCurrentFile(File disk) {
                return Objects.equals(disk.getPath(), currentFilePath);
            }

            @Override
            public void diskAlmostFull(File disk) {
                if (!isCurrentFile(disk)) {
                    return;
                }
                if (gcThread.isForceGCAllowWhenNoSpace()) {
                    gcThread.enableForceGC();
                } else {
                    gcThread.suspendMajorGC();
                }
            }

            @Override
            public void diskFull(File disk) {
                if (!isCurrentFile(disk)) {
                    return;
                }
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
                if (!isCurrentFile(disk)) {
                    return;
                }
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
                if (!isCurrentFile(disk)) {
                    return;
                }
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
        log.debug().attr("ledgerId", ledgerId).log("setLimboState");
        ledgerIndex.setLimbo(ledgerId);
    }

    @Override
    public boolean hasLimboState(long ledgerId) throws IOException {
        log.debug().attr("ledgerId", ledgerId).log("hasLimboState");
        return ledgerIndex.get(ledgerId).getLimbo();
    }

    @Override
    public void clearLimboState(long ledgerId) throws IOException {
        log.debug().attr("ledgerId", ledgerId).log("clearLimboState");
        ledgerIndex.clearLimbo(ledgerId);
    }

    private void throwIfLimbo(long ledgerId) throws IOException, BookieException {
        if (hasLimboState(ledgerId)) {
            log.debug().attr("ledgerId", ledgerId).log("Accessing ledger in limbo state, throwing exception");
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
                log.info()
                        .attr("curFlags", curFlags)
                        .attr("newFlags", newFlags)
                        .log("Conflict updating storage state flags, retrying");
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
                log.info()
                        .attr("curFlags", curFlags)
                        .attr("newFlags", newFlags)
                        .log("Conflict updating storage state flags, retrying");
            }
        }
    }

    @VisibleForTesting
    DbLedgerStorageStats getDbLedgerStorageStats() {
        return dbLedgerStorageStats;
    }

    @VisibleForTesting
    public String getLedgerBaseDir() {
        return ledgerBaseDir;
    }

    @VisibleForTesting
    public String getIndexBaseDir() {
        return indexBaseDir;
    }
}
