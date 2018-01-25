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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.SortedMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.Checkpointer;
import org.apache.bookkeeper.bookie.CompactableLedgerStorage;
import org.apache.bookkeeper.bookie.EntryLocation;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.GarbageCollectorThread;
import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.StateManager;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.Batch;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.common.util.Watcher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of LedgerStorage that uses RocksDB to keep the indexes for
 * entries stored in EntryLogs.
 */
public class DbLedgerStorage implements CompactableLedgerStorage {

    private EntryLogger entryLogger;

    private LedgerMetadataIndex ledgerIndex;
    private EntryLocationIndex entryLocationIndex;

    private GarbageCollectorThread gcThread;

    // Write cache where all new entries are inserted into
    protected WriteCache writeCache;

    // Write cache that is used to swap with writeCache during flushes
    protected WriteCache writeCacheBeingFlushed;

    // Cache where we insert entries for speculative reading
    private ReadCache readCache;

    private final ReentrantReadWriteLock writeCacheMutex = new ReentrantReadWriteLock();
    private final Condition flushWriteCacheCondition = writeCacheMutex.writeLock().newCondition();

    protected final ReentrantLock flushMutex = new ReentrantLock();

    protected final AtomicBoolean hasFlushBeenTriggered = new AtomicBoolean(false);
    private final AtomicBoolean isFlushOngoing = new AtomicBoolean(false);

    private final ExecutorService executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("db-storage"));

    // Executor used to for db index cleanup
    private final ExecutorService cleanupExecutor = Executors
            .newSingleThreadExecutor(new DefaultThreadFactory("db-storage-cleanup"));

    static final String WRITE_CACHE_MAX_SIZE_MB = "dbStorage_writeCacheMaxSizeMb";
    static final String READ_AHEAD_CACHE_BATCH_SIZE = "dbStorage_readAheadCacheBatchSize";
    static final String READ_AHEAD_CACHE_MAX_SIZE_MB = "dbStorage_readAheadCacheMaxSizeMb";

    private static final long DEFAULT_WRITE_CACHE_MAX_SIZE_MB = 16;
    private static final long DEFAULT_READ_CACHE_MAX_SIZE_MB = 16;
    private static final int DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE = 100;

    private static final int MB = 1024 * 1024;

    private final CopyOnWriteArrayList<LedgerDeletionListener> ledgerDeletionListeners = Lists
            .newCopyOnWriteArrayList();

    private long writeCacheMaxSize;

    private CheckpointSource checkpointSource = null;
    private Checkpoint lastCheckpoint = Checkpoint.MIN;

    private long readCacheMaxSize;
    private int readAheadCacheBatchSize;

    private StatsLogger stats;

    private OpStatsLogger addEntryStats;
    private OpStatsLogger readEntryStats;
    private OpStatsLogger readCacheHitStats;
    private OpStatsLogger readCacheMissStats;
    private OpStatsLogger readAheadBatchCountStats;
    private OpStatsLogger readAheadBatchSizeStats;
    private OpStatsLogger flushStats;
    private OpStatsLogger flushSizeStats;

    @Override
    public void initialize(ServerConfiguration conf, LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager,
        LedgerDirsManager indexDirsManager, StateManager stateManager, CheckpointSource checkpointSource,
                           Checkpointer checkpointer, StatsLogger statsLogger) throws IOException {
        checkArgument(ledgerDirsManager.getAllLedgerDirs().size() == 1,
                "Db implementation only allows for one storage dir");

        String baseDir = ledgerDirsManager.getAllLedgerDirs().get(0).toString();

        writeCacheMaxSize = conf.getLong(WRITE_CACHE_MAX_SIZE_MB, DEFAULT_WRITE_CACHE_MAX_SIZE_MB) * MB;

        writeCache = new WriteCache(writeCacheMaxSize / 2);
        writeCacheBeingFlushed = new WriteCache(writeCacheMaxSize / 2);

        this.checkpointSource = checkpointSource;

        readCacheMaxSize = conf.getLong(READ_AHEAD_CACHE_MAX_SIZE_MB, DEFAULT_READ_CACHE_MAX_SIZE_MB) * MB;
        readAheadCacheBatchSize = conf.getInt(READ_AHEAD_CACHE_BATCH_SIZE, DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE);

        readCache = new ReadCache(readCacheMaxSize);

        this.stats = statsLogger;

        log.info("Started Db Ledger Storage");
        log.info(" - Write cache size: {} MB", writeCacheMaxSize / MB);
        log.info(" - Read Cache: {} MB", readCacheMaxSize / MB);
        log.info(" - Read Ahead Batch size: : {}", readAheadCacheBatchSize);

        ledgerIndex = new LedgerMetadataIndex(conf, KeyValueStorageRocksDB.factory, baseDir, stats);
        entryLocationIndex = new EntryLocationIndex(conf, KeyValueStorageRocksDB.factory, baseDir, stats);

        entryLogger = new EntryLogger(conf, ledgerDirsManager);
        gcThread = new GarbageCollectorThread(conf, ledgerManager, this, statsLogger);

        registerStats();
    }

    public void registerStats() {
        stats.registerGauge("write-cache-size", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return writeCache.size() + writeCacheBeingFlushed.size();
            }
        });
        stats.registerGauge("write-cache-count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return writeCache.count() + writeCacheBeingFlushed.count();
            }
        });
        stats.registerGauge("read-cache-size", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return readCache.size();
            }
        });
        stats.registerGauge("read-cache-count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return readCache.count();
            }
        });

        addEntryStats = stats.getOpStatsLogger("add-entry");
        readEntryStats = stats.getOpStatsLogger("read-entry");
        readCacheHitStats = stats.getOpStatsLogger("read-cache-hits");
        readCacheMissStats = stats.getOpStatsLogger("read-cache-misses");
        readAheadBatchCountStats = stats.getOpStatsLogger("readahead-batch-count");
        readAheadBatchSizeStats = stats.getOpStatsLogger("readahead-batch-size");
        flushStats = stats.getOpStatsLogger("flush");
        flushSizeStats = stats.getOpStatsLogger("flush-size");
    }

    @Override
    public void start() {
        gcThread.start();
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
        return ledgerIndex.setFenced(ledgerId);
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
    public long addEntry(ByteBuf entry) throws IOException {
        long startTime = MathUtils.nowInNano();

        long ledgerId = entry.readLong();
        long entryId = entry.readLong();
        entry.resetReaderIndex();

        if (log.isDebugEnabled()) {
            log.debug("Add entry. {}@{}", ledgerId, entryId);
        }

        // Waits if the write cache is being switched for a flush
        writeCacheMutex.readLock().lock();
        boolean inserted;
        try {
            inserted = writeCache.put(ledgerId, entryId, entry);
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        if (!inserted) {
            triggerFlushAndAddEntry(ledgerId, entryId, entry);
        }

        recordSuccessfulEvent(addEntryStats, startTime);
        return entryId;
    }

    private void triggerFlushAndAddEntry(long ledgerId, long entryId, ByteBuf entry) throws IOException {
        // Write cache is full, we need to trigger a flush so that it gets rotated
        writeCacheMutex.writeLock().lock();

        try {
            // If the flush has already been triggered or flush has already switched the
            // cache, we don't need to
            // trigger another flush
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

            long timeoutNs = TimeUnit.MILLISECONDS.toNanos(100);
            while (hasFlushBeenTriggered.get()) {
                if (timeoutNs <= 0L) {
                    throw new IOException("Write cache was not trigger within the timeout, cannot add entry " + ledgerId
                            + "@" + entryId);
                }
                timeoutNs = flushWriteCacheCondition.awaitNanos(timeoutNs);
            }

            if (!writeCache.put(ledgerId, entryId, entry)) {
                // Still wasn't able to cache entry
                throw new IOException("Error while inserting entry in write cache" + ledgerId + "@" + entryId);
            }

        } catch (InterruptedException e) {
            throw new IOException("Interrupted when adding entry " + ledgerId + "@" + entryId);
        } finally {
            writeCacheMutex.writeLock().unlock();
        }
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

        writeCacheMutex.readLock().lock();
        try {
            // First try to read from the write cache of recent entries
            ByteBuf entry = writeCache.get(ledgerId, entryId);
            if (entry != null) {
                recordSuccessfulEvent(readCacheHitStats, startTime);
                recordSuccessfulEvent(readEntryStats, startTime);
                return entry;
            }

            // If there's a flush going on, the entry might be in the flush buffer
            entry = writeCacheBeingFlushed.get(ledgerId, entryId);
            if (entry != null) {
                recordSuccessfulEvent(readCacheHitStats, startTime);
                recordSuccessfulEvent(readEntryStats, startTime);
                return entry;
            }
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        // Try reading from read-ahead cache
        ByteBuf entry = readCache.get(ledgerId, entryId);
        if (entry != null) {
            recordSuccessfulEvent(readCacheHitStats, startTime);
            recordSuccessfulEvent(readEntryStats, startTime);
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
            recordFailedEvent(readEntryStats, startTime);
            throw e;
        }

        readCache.put(ledgerId, entryId, entry);

        // Try to read more entries
        long nextEntryLocation = entryLocation + 4 /* size header */ + entry.readableBytes();
        fillReadAheadCache(ledgerId, entryId + 1, nextEntryLocation);

        recordSuccessfulEvent(readCacheMissStats, startTime);
        recordSuccessfulEvent(readEntryStats, startTime);
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
                ByteBuf entry = entryLogger.internalReadEntry(orginalLedgerId, -1, currentEntryLocation);

                try {
                    long currentEntryLedgerId = entry.getLong(0);
                    long currentEntryId = entry.getLong(8);

                    if (currentEntryLedgerId != orginalLedgerId) {
                        // Found an entry belonging to a different ledger, stopping read-ahead
                        entry.release();
                        return;
                    }

                    // Insert entry in read cache
                    readCache.put(orginalLedgerId, currentEntryId, entry);

                    count++;
                    size += entry.readableBytes();

                    currentEntryLocation += 4 + entry.readableBytes();
                    currentEntryLogId = currentEntryLocation >> 32;
                } finally {
                    entry.release();
                }
            }

            readAheadBatchCountStats.registerSuccessfulValue(count);
            readAheadBatchSizeStats.registerSuccessfulValue(size);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Exception during read ahead for ledger: {}: e", orginalLedgerId, e);
            }
        }
    }

    public ByteBuf getLastEntry(long ledgerId) throws IOException {
        long startTime = MathUtils.nowInNano();

        writeCacheMutex.readLock().lock();
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

                recordSuccessfulEvent(readCacheHitStats, startTime);
                recordSuccessfulEvent(readEntryStats, startTime);
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

                recordSuccessfulEvent(readCacheHitStats, startTime);
                recordSuccessfulEvent(readEntryStats, startTime);
                return entry;
            }
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        // Search the last entry in storage
        long lastEntryId = entryLocationIndex.getLastEntryInLedger(ledgerId);
        if (log.isDebugEnabled()) {
            log.debug("Found last entry for ledger {} in db: {}", ledgerId, lastEntryId);
        }

        long entryLocation = entryLocationIndex.getLocation(ledgerId, lastEntryId);
        ByteBuf content = entryLogger.readEntry(ledgerId, lastEntryId, entryLocation);

        recordSuccessfulEvent(readCacheMissStats, startTime);
        recordSuccessfulEvent(readEntryStats, startTime);
        return content;
    }

    @VisibleForTesting
    boolean isFlushRequired() {
        writeCacheMutex.readLock().lock();
        try {
            return !writeCache.isEmpty();
        } finally {
            writeCacheMutex.readLock().unlock();
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

            recordSuccessfulEvent(flushStats, startTime);
            flushSizeStats.registerSuccessfulValue(sizeToFlush);
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
        writeCacheMutex.writeLock().lock();
        try {
            // First, swap the current write-cache map with an empty one so that writes will
            // go on unaffected. Only a single flush is happening at the same time
            WriteCache tmp = writeCacheBeingFlushed;
            writeCacheBeingFlushed = writeCache;
            writeCache = tmp;

            // since the cache is switched, we can allow flush to be triggered
            hasFlushBeenTriggered.set(false);
            flushWriteCacheCondition.signalAll();
        } finally {
            try {
                isFlushOngoing.set(true);
            } finally {
                writeCacheMutex.writeLock().unlock();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        checkpoint(Checkpoint.MAX);
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Deleting ledger {}", ledgerId);
        }

        // Delete entries from this ledger that are still in the write cache
        writeCacheMutex.readLock().lock();
        try {
            writeCache.deleteLedger(ledgerId);
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        entryLocationIndex.delete(ledgerId);
        ledgerIndex.delete(ledgerId);

        for (int i = 0, size = ledgerDeletionListeners.size(); i < size; i++) {
            LedgerDeletionListener listener = ledgerDeletionListeners.get(i);
            listener.ledgerDeleted(ledgerId);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean waitForLastAddConfirmedUpdate(long ledgerId, long previousLAC,
            Watcher<LastAddConfirmedUpdateNotification> watcher) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setExplicitlac(long ledgerId, ByteBuf lac) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flushEntriesLocationsIndex() throws IOException {
        // No-op. Location index is already flushed in updateEntriesLocations() call
    }

    /**
     * Add an already existing ledger to the index.
     *
     * <p>This method is only used as a tool to help the migration from
     * InterleaveLedgerStorage to DbLedgerStorage
     *
     * @param ledgerId
     *            the ledger id
     * @param entries
     *            a map of entryId -> location
     * @return the number of
     */
    public long addLedgerToIndex(long ledgerId, boolean isFenced, byte[] masterKey,
            Iterable<SortedMap<Long, Long>> entries) throws Exception {
        LedgerData ledgerData = LedgerData.newBuilder().setExists(true).setFenced(isFenced)
                .setMasterKey(ByteString.copyFrom(masterKey)).build();
        ledgerIndex.set(ledgerId, ledgerData);
        AtomicLong numberOfEntries = new AtomicLong();

        // Iterate over all the entries pages
        Batch batch = entryLocationIndex.newBatch();
        entries.forEach(map -> {
            map.forEach((entryId, location) -> {
                try {
                    entryLocationIndex.addLocation(batch, ledgerId, entryId, location);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                numberOfEntries.incrementAndGet();
            });
        });

        batch.flush();
        batch.close();

        return numberOfEntries.get();
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

    /**
     * Reads ledger index entries to get list of entry-logger that contains given ledgerId.
     *
     * @param ledgerId
     * @param serverConf
     * @param processor
     * @throws IOException
     */
    public static void readLedgerIndexEntries(long ledgerId, ServerConfiguration serverConf,
            LedgerLoggerProcessor processor) throws IOException {

        checkNotNull(serverConf, "ServerConfiguration can't be null");
        checkNotNull(processor, "LedgerLoggger info processor can't null");

        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(serverConf, serverConf.getLedgerDirs(),
            new DiskChecker(serverConf.getDiskUsageThreshold(), serverConf.getDiskUsageWarnThreshold()));
        String ledgerBasePath = ledgerDirsManager.getAllLedgerDirs().get(0).toString();

        EntryLocationIndex entryLocationIndex = new EntryLocationIndex(serverConf,
                (path, dbConfigType, conf1) -> new KeyValueStorageRocksDB(path, DbConfigType.Small, conf1, true),
                ledgerBasePath, NullStatsLogger.INSTANCE);
        try {
            long lastEntryId = entryLocationIndex.getLastEntryInLedger(ledgerId);
            for (long currentEntry = 0; currentEntry <= lastEntryId; currentEntry++) {
                long offset = entryLocationIndex.getLocation(ledgerId, currentEntry);
                if (offset <= 0) {
                    // entry not found in this bookie
                    continue;
                }
                long entryLogId = offset >> 32L;
                long position = offset & 0xffffffffL;
                processor.process(currentEntry, entryLogId, position);
            }
        } finally {
            entryLocationIndex.close();
        }
    }

    /**
     * Interface which process ledger logger.
     */
    public interface LedgerLoggerProcessor {
        void process(long entryId, long entryLogId, long position);
    }

    private static final Logger log = LoggerFactory.getLogger(DbLedgerStorage.class);
}
