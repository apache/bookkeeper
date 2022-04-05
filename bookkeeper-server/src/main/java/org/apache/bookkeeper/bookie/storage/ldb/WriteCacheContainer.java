package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.ThreadRegistry;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

public class WriteCacheContainer {
    private final StampedLock writeCacheRotationLock = new StampedLock();

    // Write cache where all new entries are inserted into
    protected volatile WriteCache writeCache;

    private volatile LinkedBlockingQueue<WriteCache> freeCaches = new LinkedBlockingQueue<>();
    private volatile LinkedBlockingQueue<WriteCache> fullCaches = new LinkedBlockingQueue<>();

    private static final Logger log = LoggerFactory.getLogger(WriteCacheContainer.class);

    private DbLedgerStorageStats dbLedgerStorageStats;

    private final long maxThrottleTimeNanos;
    protected final AtomicBoolean hasFlushBeenTriggered = new AtomicBoolean(false);
    private final AtomicBoolean isFlushOngoing = new AtomicBoolean(false);

    private static String dbStoragerExecutorName = "db-storage";
    private final ExecutorService executor = Executors.newSingleThreadExecutor(
            new DefaultThreadFactory(dbStoragerExecutorName));


    private final Counter flushExecutorTime;

    private CheckpointSource checkpointSource = CheckpointSource.DEFAULT;
    private CheckpointSource.Checkpoint lastCheckpoint = CheckpointSource.Checkpoint.MIN;

    private final ConcurrentLongHashMap<TransientLedgerInfo> transientLedgerInfoCache;
    private final ReentrantLock flushMutex;
    private final EntryLocationIndex entryLocationIndex;
    private final EntryLogger entryLogger;
    private final LedgerMetadataIndex ledgerIndex;
    // Executor used to for db index cleanup
    private final ScheduledExecutorService cleanupExecutor = Executors
            .newSingleThreadScheduledExecutor(new DefaultThreadFactory("db-storage-cleanup"));
    private final long writeCacheMaxSize;


    public WriteCacheContainer(ByteBufAllocator allocator, long writeCacheSize, int writeCacheNum,
                               long maxThrottleTimeNanos, StatsLogger ledgerDirStatsLogger,
                               ConcurrentLongHashMap<TransientLedgerInfo> transientLedgerInfoCache,
                               EntryLocationIndex entryLocationIndex,
                               EntryLogger entryLogger,
                               LedgerMetadataIndex ledgerIndex,
                               ReentrantLock flushMutex) {
        this.writeCacheMaxSize = writeCacheSize;
        for (int i = 0; i < writeCacheNum; i++) {
            freeCaches.add(new WriteCache(allocator, writeCacheMaxSize / writeCacheNum));
        }
        this.writeCache = freeCaches.poll();

        this.maxThrottleTimeNanos = maxThrottleTimeNanos;
        this.transientLedgerInfoCache = transientLedgerInfoCache;
        this.entryLocationIndex = entryLocationIndex;
        this.entryLogger = entryLogger;
        this.ledgerIndex = ledgerIndex;
        this.flushMutex = flushMutex;

        flushExecutorTime = ledgerDirStatsLogger.getThreadScopedCounter("db-storage-thread-time");

        executor.submit(() -> {
            ThreadRegistry.register(dbStoragerExecutorName, 0);
            // ensure the metric gets registered on start-up as this thread only executes
            // when the write cache is full which may not happen or not for a long time
            flushExecutorTime.add(0);
        });
        executor.execute(() -> flushCache());
    }


    public void flushCache() {
        while (true) {
            try {
                WriteCache writeCacheBeingFlushed = fullCaches.take();
                KeyValueStorage.Batch batch = entryLocationIndex.newBatch();
                writeCacheBeingFlushed.forEach((ledgerId, entryId, entry) -> {
                    try {
                        long location = entryLogger.addEntry(ledgerId, entry, true);
                        entryLocationIndex.addLocation(batch, ledgerId, entryId, location);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                long entryLoggerStart = MathUtils.nowInNano();
                entryLogger.flush();
                recordSuccessfulEvent(dbLedgerStorageStats.getFlushEntryLogStats(), entryLoggerStart);

                long batchFlushStartTime = MathUtils.nowInNano();
                batch.flush();
                batch.close();
                recordSuccessfulEvent(dbLedgerStorageStats.getFlushLocationIndexStats(), batchFlushStartTime);
                if (log.isDebugEnabled()) {
                    log.debug("DB batch flushed time : {} s",
                            MathUtils.elapsedNanos(batchFlushStartTime) / (double) TimeUnit.SECONDS.toNanos(1));
                }

                long ledgerIndexStartTime = MathUtils.nowInNano();
                ledgerIndex.flush();
                recordSuccessfulEvent(dbLedgerStorageStats.getFlushLedgerIndexStats(), ledgerIndexStartTime);

                // Discard all the entry from the write cache, since they're now persisted
                writeCacheBeingFlushed.clear();
                freeCaches.add(writeCacheBeingFlushed);
            } catch (IOException e) {
                log.error("Flush failed!", e);
            } catch (InterruptedException e) {
                log.warn("Flush Interrupted!", e);
                break;
            }
        }
    }

    public void setDbLedgerStorageStats(DbLedgerStorageStats dbLedgerStorageStats) {
        this.dbLedgerStorageStats = dbLedgerStorageStats;
    }

    public long size() {
        long isFlushingSize = 0;
        for (WriteCache cache : fullCaches) {
            isFlushingSize += cache.size();
        }
        return writeCache.size() + isFlushingSize;
    }

    public long count() {
        long isFlushingCount = 0;
        for (WriteCache cache : fullCaches) {
            isFlushingCount += cache.count();
        }
        return writeCache.count() + isFlushingCount;
    }


    public void close() {
        writeCache.close();
        for (WriteCache cache : fullCaches) {
            cache.close();
        }
    }

    public boolean entryExists(long ledgerId, long entryId) {

        // We need to try to read from both write caches, since recent entries could be found in either of the two. The
        // write caches are already thread safe on their own, here we just need to make sure we get references to both
        // of them. Using an optimistic lock since the read lock is always free, unless we're swapping the caches.
        long stamp = writeCacheRotationLock.tryOptimisticRead();
        WriteCache localWriteCache = writeCache;
        LinkedBlockingQueue<WriteCache> localFullCaches = fullCaches;
        if (!writeCacheRotationLock.validate(stamp)) {
            // Fallback to regular read lock approach
            stamp = writeCacheRotationLock.readLock();
            try {
                localWriteCache = writeCache;
                localFullCaches = fullCaches;
            } finally {
                writeCacheRotationLock.unlockRead(stamp);
            }
        }
        if (localWriteCache.hasEntry(ledgerId, entryId)) {
            return true;
        }

        for (WriteCache cache : fullCaches) {
            if (cache.hasEntry(ledgerId, entryId)) {
                return true;
            }
        }

        return false;
    }

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
                        log.error("Error during flush", e);
                    } finally {
                        flushExecutorTime.add(MathUtils.elapsedNanos(startTime));
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
        throw new BookieException.OperationRejectedException();
    }

    public void flush() throws IOException {
        CheckpointSource.Checkpoint cp = checkpointSource.newCheckpoint();
        checkpoint(cp);
        checkpointSource.checkpointComplete(cp, true);
    }

    private void recordSuccessfulEvent(OpStatsLogger logger, long startTimeNanos) {
        logger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
    }

    private void recordFailedEvent(OpStatsLogger logger, long startTimeNanos) {
        logger.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
    }

    private void updateCachedLacIfNeeded(long ledgerId, long lac) {
        TransientLedgerInfo tli = transientLedgerInfoCache.get(ledgerId);
        if (tli != null) {
            tli.setLastAddConfirmed(lac);
        }
    }

    public long getFlushingSize() {
        long flushingSize = 0;
        for (WriteCache cache : fullCaches) {
            flushingSize += cache.size();
        }
        return flushingSize;
    }


    public long getFlushingCount() {
        long flushingCount = 0;
        for (WriteCache cache : fullCaches) {
            flushingCount += cache.count();
        }
        return flushingCount;
    }
    

    public void checkpoint(CheckpointSource.Checkpoint checkpoint) throws IOException {
        CheckpointSource.Checkpoint thisCheckpoint = checkpointSource.newCheckpoint();
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

            long sizeToFlush = getFlushingSize();
            if (log.isDebugEnabled()) {
                log.debug("Flushing entries. count: {} -- size {} Mb", getFlushingCount(),
                        sizeToFlush / 1024.0 / 1024);
            }


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

            double flushTimeSeconds = MathUtils.elapsedNanos(startTime) / (double) TimeUnit.SECONDS.toNanos(1);
            double flushThroughput = sizeToFlush / 1024.0 / 1024.0 / flushTimeSeconds;

            if (log.isDebugEnabled()) {
                log.debug("Flushing done time {} s -- Written {} MB/s", flushTimeSeconds, flushThroughput);
            }

            recordSuccessfulEvent(dbLedgerStorageStats.getFlushStats(), startTime);
            dbLedgerStorageStats.getFlushSizeStats().registerSuccessfulValue(sizeToFlush);
        } catch (RuntimeException e) {
            recordFailedEvent(dbLedgerStorageStats.getFlushStats(), startTime);
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
    protected void swapWriteCache() {
        long stamp = writeCacheRotationLock.writeLock();
        try {
            // First, swap the current write-cache map with an empty one so that writes will
            // go on unaffected. Only a single flush is happening at the same time
            WriteCache tmp = freeCaches.poll();
            if (tmp == null) {
                return;
            }
            fullCaches.add(writeCache);
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


    public ByteBuf getLastEntry(long ledgerId) {

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

                dbLedgerStorageStats.getWriteCacheHitCounter().inc();
                return entry;
            }

            // If there's a flush going on, the entry might be in the flush buffer

            for (WriteCache writeCacheBeingFlushed : fullCaches) {
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

                    dbLedgerStorageStats.getWriteCacheHitCounter().inc();
                    return entry;
                }
            }

            dbLedgerStorageStats.getWriteCacheMissCounter().inc();
            return entry;
        } finally {
            writeCacheRotationLock.unlockRead(stamp);
        }
    }


    public ByteBuf doGetEntry(long ledgerId, long entryId) throws IOException, BookieException {
        // We need to try to read from both write caches, since recent entries could be found in either of the two. The
        // write caches are already thread safe on their own, here we just need to make sure we get references to both
        // of them. Using an optimistic lock since the read lock is always free, unless we're swapping the caches.
        long stamp = writeCacheRotationLock.tryOptimisticRead();
        WriteCache localWriteCache = writeCache;
        LinkedBlockingQueue<WriteCache> localFullCaches = fullCaches;
        if (!writeCacheRotationLock.validate(stamp)) {
            // Fallback to regular read lock approach
            stamp = writeCacheRotationLock.readLock();
            try {
                localWriteCache = writeCache;
                localFullCaches = fullCaches;
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

        for (WriteCache localWriteCacheBeingFlushed : localFullCaches) {
            entry = localWriteCacheBeingFlushed.get(ledgerId, entryId);
            if (entry != null) {
                dbLedgerStorageStats.getWriteCacheHitCounter().inc();
                return entry;
            }
        }

        dbLedgerStorageStats.getWriteCacheMissCounter().inc();

        return entry;
    }


    boolean isFlushRequired() {
        long stamp = writeCacheRotationLock.readLock();
        try {
            return !writeCache.isEmpty();
        } finally {
            writeCacheRotationLock.unlockRead(stamp);
        }
    }


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
    }

    public void shutdown() throws InterruptedException {
        close();
        executor.shutdown();
    }


}
