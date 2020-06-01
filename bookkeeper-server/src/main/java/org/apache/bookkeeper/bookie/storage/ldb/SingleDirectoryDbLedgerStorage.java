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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.PrimitiveIterator.OfLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.OperationRejectedException;
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
import org.apache.bookkeeper.bookie.LedgerEntryPage;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
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

    private final ExecutorService executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("db-storage"));

    // Executor used to for db index cleanup
    private final ScheduledExecutorService cleanupExecutor = Executors
            .newSingleThreadScheduledExecutor(new DefaultThreadFactory("db-storage-cleanup"));

    private final CopyOnWriteArrayList<LedgerDeletionListener> ledgerDeletionListeners = Lists
            .newCopyOnWriteArrayList();

    private final CheckpointSource checkpointSource;
    private Checkpoint lastCheckpoint = Checkpoint.MIN;

    private final long writeCacheMaxSize;
    private final long readCacheMaxSize;
    private final int readAheadCacheBatchSize;

    private final long maxThrottleTimeNanos;

    private final DbLedgerStorageStats dbLedgerStorageStats;

    static final String READ_AHEAD_CACHE_BATCH_SIZE = "dbStorage_readAheadCacheBatchSize";
    private static final int DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE = 100;

    private static final long DEFAULT_MAX_THROTTLE_TIME_MILLIS = TimeUnit.SECONDS.toMillis(10);

    public SingleDirectoryDbLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
            LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager, StateManager stateManager,
            CheckpointSource checkpointSource, Checkpointer checkpointer, StatsLogger statsLogger,
            ByteBufAllocator allocator, ScheduledExecutorService gcExecutor, long writeCacheSize, long readCacheSize)
            throws IOException {

        checkArgument(ledgerDirsManager.getAllLedgerDirs().size() == 1,
                "Db implementation only allows for one storage dir");

        String baseDir = ledgerDirsManager.getAllLedgerDirs().get(0).toString();
        log.info("Creating single directory db ledger storage on {}", baseDir);

        this.writeCacheMaxSize = writeCacheSize;
        this.writeCache = new WriteCache(allocator, writeCacheMaxSize / 2);
        this.writeCacheBeingFlushed = new WriteCache(allocator, writeCacheMaxSize / 2);

        this.checkpointSource = checkpointSource;

        readCacheMaxSize = readCacheSize;
        readAheadCacheBatchSize = conf.getInt(READ_AHEAD_CACHE_BATCH_SIZE, DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE);

        long maxThrottleTimeMillis = conf.getLong(DbLedgerStorage.MAX_THROTTLE_TIME_MILLIS,
                DEFAULT_MAX_THROTTLE_TIME_MILLIS);
        maxThrottleTimeNanos = TimeUnit.MILLISECONDS.toNanos(maxThrottleTimeMillis);

        readCache = new ReadCache(allocator, readCacheMaxSize);

        ledgerIndex = new LedgerMetadataIndex(conf, KeyValueStorageRocksDB.factory, baseDir, statsLogger);
        entryLocationIndex = new EntryLocationIndex(conf, KeyValueStorageRocksDB.factory, baseDir, statsLogger);

        transientLedgerInfoCache = new ConcurrentLongHashMap<>(16 * 1024,
                Runtime.getRuntime().availableProcessors() * 2);
        cleanupExecutor.scheduleAtFixedRate(this::cleanupStaleTransientLedgerInfo,
                TransientLedgerInfo.LEDGER_INFO_CACHING_TIME_MINUTES,
                TransientLedgerInfo.LEDGER_INFO_CACHING_TIME_MINUTES, TimeUnit.MINUTES);

        entryLogger = new EntryLogger(conf, ledgerDirsManager, null, statsLogger, allocator);
        gcThread = new GarbageCollectorThread(conf, ledgerManager, this, statsLogger);

        dbLedgerStorageStats = new DbLedgerStorageStats(
            statsLogger,
            () -> writeCache.size() + writeCacheBeingFlushed.size(),
            () -> writeCache.count() + writeCacheBeingFlushed.count(),
            () -> readCache.size(),
            () -> readCache.count()
        );
    }

    @Override
    public void initialize(ServerConfiguration conf, LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager,
            LedgerDirsManager indexDirsManager, StateManager stateManager, CheckpointSource checkpointSource,
            Checkpointer checkpointer, StatsLogger statsLogger,
            ByteBufAllocator allocator) throws IOException {
        /// Initialized in constructor
    }

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

            writeCache.close();
            writeCacheBeingFlushed.close();
            readCache.close();
            executor.shutdown();

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
    public boolean isFenced(long ledgerId) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("isFenced. ledger: {}", ledgerId);
        }
        return ledgerIndex.get(ledgerId).getFenced();
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
        long startTime = MathUtils.nowInNano();

        long ledgerId = entry.getLong(entry.readerIndex());
        long entryId = entry.getLong(entry.readerIndex() + 8);
        long lac = entry.getLong(entry.readerIndex() + 16);

        if (log.isDebugEnabled()) {
            log.debug("Add entry. {}@{}, lac = {}", ledgerId, entryId, lac);
        }

        // First we try to do an optimistic locking to get access to the current write cache.
        // This is based on the fact that the write cache is only being rotated (swapped) every 1 minute. During the
        // rest of the time, we can have multiple thread using the optimistic lock here without interfering.
        long stamp = writeCacheRotationLock.tryOptimisticRead();
        boolean inserted = false;

        inserted = writeCache.put(ledgerId, entryId, entry);
        if (!writeCacheRotationLock.validate(stamp)) {
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
        // Write cache is full, we need to trigger a flush so that it gets rotated
        // If the flush has already been triggered or flush has already switched the
        // cache, we don't need to trigger another flush
        if (!isFlushOngoing.get() && hasFlushBeenTriggered.compareAndSet(false, true)) {
            // Trigger an early flush in background
            log.info("Write cache is full, triggering flush");
            executor.execute(() -> {
                try {
                    flush();
                } catch (IOException e) {
                    log.error("Error during flush", e);
                }
            });
        }

        dbLedgerStorageStats.getThrottledWriteRequests().inc();
        long absoluteTimeoutNanos = System.nanoTime() + maxThrottleTimeNanos;

        while (System.nanoTime() < absoluteTimeoutNanos) {
            long stamp = writeCacheRotationLock.readLock();
            try {
                if (writeCache.put(ledgerId, entryId, entry)) {
                    // We succeeded in putting the entry in write cache in the
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
        throw new OperationRejectedException();
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException {
        long startTime = MathUtils.nowInNano();
        if (log.isDebugEnabled()) {
            log.debug("Get Entry: {}@{}", ledgerId, entryId);
        }

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
            recordSuccessfulEvent(dbLedgerStorageStats.getReadCacheHitStats(), startTime);
            recordSuccessfulEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
            return entry;
        }

        // If there's a flush going on, the entry might be in the flush buffer
        entry = localWriteCacheBeingFlushed.get(ledgerId, entryId);
        if (entry != null) {
            recordSuccessfulEvent(dbLedgerStorageStats.getReadCacheHitStats(), startTime);
            recordSuccessfulEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
            return entry;
        }

        // Try reading from read-ahead cache
        entry = readCache.get(ledgerId, entryId);
        if (entry != null) {
            recordSuccessfulEvent(dbLedgerStorageStats.getReadCacheHitStats(), startTime);
            recordSuccessfulEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
            return entry;
        }

        // Read from main storage
        long entryLocation;
        try {
            entryLocation = entryLocationIndex.getLocation(ledgerId, entryId);
            if (entryLocation == 0) {
                throw new NoEntryException(ledgerId, entryId);
            }
            entry = entryLogger.readEntry(ledgerId, entryId, entryLocation);
        } catch (NoEntryException e) {
            recordFailedEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
            throw e;
        }

        readCache.put(ledgerId, entryId, entry);

        // Try to read more entries
        long nextEntryLocation = entryLocation + 4 /* size header */ + entry.readableBytes();
        fillReadAheadCache(ledgerId, entryId + 1, nextEntryLocation);

        recordSuccessfulEvent(dbLedgerStorageStats.getReadCacheMissStats(), startTime);
        recordSuccessfulEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
        return entry;
    }

    private void fillReadAheadCache(long orginalLedgerId, long firstEntryId, long firstEntryLocation) {
        try {
            long firstEntryLogId = (firstEntryLocation >> 32);
            long currentEntryLogId = firstEntryLogId;
            long currentEntryLocation = firstEntryLocation;
            int count = 0;
            long size = 0;

            while (count < readAheadCacheBatchSize && currentEntryLogId == firstEntryLogId) {
                ByteBuf entry = entryLogger.internalReadEntry(orginalLedgerId, firstEntryId, currentEntryLocation,
                        false /* validateEntry */);

                try {
                    long currentEntryLedgerId = entry.getLong(0);
                    long currentEntryId = entry.getLong(8);

                    if (currentEntryLedgerId != orginalLedgerId) {
                        // Found an entry belonging to a different ledger, stopping read-ahead
                        return;
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

            dbLedgerStorageStats.getReadAheadBatchCountStats().registerSuccessfulValue(count);
            dbLedgerStorageStats.getReadAheadBatchSizeStats().registerSuccessfulValue(size);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Exception during read ahead for ledger: {}: e", orginalLedgerId, e);
            }
        }
    }

    public ByteBuf getLastEntry(long ledgerId) throws IOException {
        long startTime = MathUtils.nowInNano();

        long stamp = writeCacheRotationLock.readLock();
        try {
            // First try to read from the write cache of recent entries
            ByteBuf entry = writeCache.getLastEntry(ledgerId);
            if (entry != null) {
                if (log.isDebugEnabled()) {
                    long foundLedgerId = entry.readLong(); // ledgedId
                    long entryId = entry.readLong();
                    entry.resetReaderIndex();
                    if (log.isDebugEnabled()) {
                        log.debug("Found last entry for ledger {} in write cache: {}@{}", ledgerId, foundLedgerId,
                                entryId);
                    }
                }

                recordSuccessfulEvent(dbLedgerStorageStats.getReadCacheHitStats(), startTime);
                recordSuccessfulEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
                return entry;
            }

            // If there's a flush going on, the entry might be in the flush buffer
            entry = writeCacheBeingFlushed.getLastEntry(ledgerId);
            if (entry != null) {
                if (log.isDebugEnabled()) {
                    entry.readLong(); // ledgedId
                    long entryId = entry.readLong();
                    entry.resetReaderIndex();
                    if (log.isDebugEnabled()) {
                        log.debug("Found last entry for ledger {} in write cache being flushed: {}", ledgerId, entryId);
                    }
                }

                recordSuccessfulEvent(dbLedgerStorageStats.getReadCacheHitStats(), startTime);
                recordSuccessfulEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
                return entry;
            }
        } finally {
            writeCacheRotationLock.unlockRead(stamp);
        }

        // Search the last entry in storage
        long lastEntryId = entryLocationIndex.getLastEntryInLedger(ledgerId);
        if (log.isDebugEnabled()) {
            log.debug("Found last entry for ledger {} in db: {}", ledgerId, lastEntryId);
        }

        long entryLocation = entryLocationIndex.getLocation(ledgerId, lastEntryId);
        ByteBuf content = entryLogger.readEntry(ledgerId, lastEntryId, entryLocation);

        recordSuccessfulEvent(dbLedgerStorageStats.getReadCacheMissStats(), startTime);
        recordSuccessfulEvent(dbLedgerStorageStats.getReadEntryStats(), startTime);
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

        long startTime = MathUtils.nowInNano();

        // Only a single flush operation can happen at a time
        flushMutex.lock();

        try {
            // Swap the write cache so that writes can continue to happen while the flush is
            // ongoing
            swapWriteCache();

            long sizeToFlush = writeCacheBeingFlushed.size();
            if (log.isDebugEnabled()) {
                log.debug("Flushing entries. count: {} -- size {} Mb", writeCacheBeingFlushed.count(),
                        sizeToFlush / 1024.0 / 1024);
            }

            // Write all the pending entries into the entry logger and collect the offset
            // position for each entry

            Batch batch = entryLocationIndex.newBatch();
            writeCacheBeingFlushed.forEach((ledgerId, entryId, entry) -> {
                try {
                    long location = entryLogger.addEntry(ledgerId, entry, true);
                    entryLocationIndex.addLocation(batch, ledgerId, entryId, location);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            entryLogger.flush();

            long batchFlushStarTime = System.nanoTime();
            batch.flush();
            batch.close();
            if (log.isDebugEnabled()) {
                log.debug("DB batch flushed time : {} s",
                        MathUtils.elapsedNanos(batchFlushStarTime) / (double) TimeUnit.SECONDS.toNanos(1));
            }

            ledgerIndex.flush();

            cleanupExecutor.execute(() -> {
                // There can only be one single cleanup task running because the cleanupExecutor
                // is single-threaded
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Removing deleted ledgers from db indexes");
                    }

                    entryLocationIndex.removeOffsetFromDeletedLedgers();
                    ledgerIndex.removeDeletedLedgers();
                } catch (Throwable t) {
                    log.warn("Failed to cleanup db indexes", t);
                }
            });

            lastCheckpoint = thisCheckpoint;

            // Discard all the entry from the write cache, since they're now persisted
            writeCacheBeingFlushed.clear();

            double flushTimeSeconds = MathUtils.elapsedNanos(startTime) / (double) TimeUnit.SECONDS.toNanos(1);
            double flushThroughput = sizeToFlush / 1024.0 / 1024.0 / flushTimeSeconds;

            if (log.isDebugEnabled()) {
                log.debug("Flushing done time {} s -- Written {} MB/s", flushTimeSeconds, flushThroughput);
            }

            recordSuccessfulEvent(dbLedgerStorageStats.getFlushStats(), startTime);
            dbLedgerStorageStats.getFlushSizeStats().registerSuccessfulValue(sizeToFlush);
        } catch (IOException e) {
            // Leave IOExecption as it is
            throw e;
        } catch (RuntimeException e) {
            // Wrap unchecked exceptions
            throw new IOException(e);
        } finally {
            try {
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

            // since the cache is switched, we can allow flush to be triggered
            hasFlushBeenTriggered.set(false);
        } finally {
            try {
                isFlushOngoing.set(true);
            } finally {
                writeCacheRotationLock.unlockWrite(stamp);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        Checkpoint cp = checkpointSource.newCheckpoint();
        checkpoint(cp);
        checkpointSource.checkpointComplete(cp, true);
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Deleting ledger {}", ledgerId);
        }

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
        // Trigger a flush to have all the entries being compacted in the db storage
        flush();

        entryLocationIndex.updateLocations(locations);
    }

    @Override
    public EntryLogger getEntryLogger() {
        return entryLogger;
    }

    @Override
    public long getLastAddConfirmed(long ledgerId) throws IOException {
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
    public void setExplicitlac(long ledgerId, ByteBuf lac) throws IOException {
        getOrAddLedgerInfo(ledgerId).setExplicitLac(lac);
    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) {
        TransientLedgerInfo ledgerInfo = transientLedgerInfoCache.get(ledgerId);
        if (null == ledgerInfo) {
            return null;
        } else {
            return ledgerInfo.getExplicitLac();
        }
    }

    private TransientLedgerInfo getOrAddLedgerInfo(long ledgerId) {
        TransientLedgerInfo tli = transientLedgerInfoCache.get(ledgerId);
        if (tli != null) {
            return tli;
        } else {
            TransientLedgerInfo newTli = new TransientLedgerInfo(ledgerId, ledgerIndex);
            tli = transientLedgerInfoCache.putIfAbsent(ledgerId, newTli);
            if (tli != null) {
                newTli.close();
                return tli;
            } else {
                return newTli;
            }
        }
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

    long getWriteCacheSize() {
        return writeCache.size() + writeCacheBeingFlushed.size();
    }

    long getWriteCacheCount() {
        return writeCache.count() + writeCacheBeingFlushed.count();
    }

    long getReadCacheSize() {
        return readCache.size();
    }

    long getReadCacheCount() {
        return readCache.count();
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
}
