/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import org.apache.bookkeeper.bookie.BookieCriticalThread;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.storage.EntryLogger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container for storing write cache blocks.
 */
public class WriteCacheContainer {
    protected static final Logger LOG = LoggerFactory.getLogger(WriteCacheContainer.class);

    private final LinkedBlockingQueue<WriteCache> freeBlockCaches = new LinkedBlockingQueue<>();

    protected LinkedBlockingDeque<WriteCache> fulledBlockCaches = new LinkedBlockingDeque<>();

    protected final ReentrantLock flushMutex = new ReentrantLock();

    private CheckpointSource.Checkpoint lastCheckpoint = CheckpointSource.Checkpoint.MIN;

    protected CheckpointSource checkpointSource = CheckpointSource.DEFAULT;

    private EntryLogger entryLogger;

    private LedgerMetadataIndex ledgerIndex;
    private EntryLocationIndex entryLocationIndex;

    private DbLedgerStorageStats dbLedgerStorageStats;

    protected final Counter flushExecutorTime;

    private final ScheduledExecutorService cleanupExecutor;

    private FlushWriteCacheThread flushWriteCacheThread;

    private final AtomicLong cacheSize = new AtomicLong(0);

    private final LongAdder cacheCount = new LongAdder();

    private final StampedLock rotationLock = new StampedLock();

    private final AtomicBoolean disableFlushWriteCacheThread = new AtomicBoolean(false);


    public WriteCacheContainer(EntryLogger entryLogger,
                               LedgerMetadataIndex ledgerIndex,
                               EntryLocationIndex entryLocationIndex,
                               DbLedgerStorageStats dbLedgerStorageStats,
                               ScheduledExecutorService cleanupExecutor,
                               Counter flushExecutorTime,
                               long writeCacheSize,
                               long perDirectoryBlockWriteCacheSize,
                               ByteBufAllocator allocator) {
        this.entryLogger = entryLogger;
        this.ledgerIndex = ledgerIndex;
        this.entryLocationIndex = entryLocationIndex;
        this.dbLedgerStorageStats = dbLedgerStorageStats;
        this.cleanupExecutor = cleanupExecutor;
        this.flushExecutorTime = flushExecutorTime;
        for (int i = 0; i < writeCacheSize / perDirectoryBlockWriteCacheSize; i++) {
            WriteCache writeCache = new WriteCache(allocator,
                    perDirectoryBlockWriteCacheSize);
            freeBlockCaches.add(writeCache);
        }
        flushWriteCacheThread = new FlushWriteCacheThread("flush-write-cache-thread");
        flushWriteCacheThread.start();
    }

    @VisibleForTesting
    void disableFlushWriteCacheThread() {
        disableFlushWriteCacheThread.set(true);
    }

    public long size() {
        return cacheSize.get();
    }

    public boolean hasEntry(long ledgerId, long entryId) {
        AtomicBoolean hashEntry = new AtomicBoolean(false);
        fulledBlockCaches.forEach(writeCache -> {
            if (writeCache.hasEntry(ledgerId, entryId)) {
                hashEntry.set(true);
                return;
            }
        });
        return hashEntry.get();
    }

    public ByteBuf get(long ledgerId, long entryId) {
        for (WriteCache writeCache : fulledBlockCaches) {
            ByteBuf entry = writeCache.get(ledgerId, entryId);
            if (entry != null) {
                return entry;
            }
        }
        return null;
    }

    public ByteBuf getLastEntry(long ledgerId) {
        try {
            return fulledBlockCaches.getLast().getLastEntry(ledgerId);
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    public long count() {
        return cacheCount.sum();
    }

    public boolean checkpoint(WriteCache writeCacheBeingFlushed) throws IOException {
        CheckpointSource.Checkpoint checkpoint = writeCacheBeingFlushed.getCheckpoint();
        if (lastCheckpoint.compareTo(checkpoint) > 0) {
            LOG.error("checkpoint(%s) < lastCheckpoint(%s)", checkpoint, lastCheckpoint);
            return false;
        }

        long startTime = MathUtils.nowInNano();

        try {
            long sizeToFlush = writeCacheBeingFlushed.size();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Flushing entries. count: {} -- size {} Mb",
                        writeCacheBeingFlushed.count(), sizeToFlush / 1024.0 / 1024);
            }

            // Write all the pending entries into the entry logger and collect the offset
            // position for each entry

            KeyValueStorage.Batch batch = entryLocationIndex.newBatch();
            writeCacheBeingFlushed.forEach((ledgerId, entryId, entry) -> {
                try {
                    long location = entryLogger.addEntry(ledgerId, entry);
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("DB batch flushed time : {} s",
                        MathUtils.elapsedNanos(batchFlushStartTime)
                                / (double) TimeUnit.SECONDS.toNanos(1));
            }

            long ledgerIndexStartTime = MathUtils.nowInNano();
            ledgerIndex.flush();
            recordSuccessfulEvent(dbLedgerStorageStats.getFlushLedgerIndexStats(), ledgerIndexStartTime);

            cleanupExecutor.execute(() -> {
                // There can only be one single cleanup task running because the cleanupExecutor
                // is single-threaded
                try {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Removing deleted ledgers from db indexes");
                    }

                    entryLocationIndex.removeOffsetFromDeletedLedgers();
                    ledgerIndex.removeDeletedLedgers();
                } catch (Throwable t) {
                    LOG.warn("Failed to cleanup db indexes", t);
                }
            });

            lastCheckpoint = checkpoint;
            // Discard all the entry from the write cache, since they're now persisted
            writeCacheBeingFlushed.clear();
            if (writeCacheBeingFlushed.isCommonWriteCache()) {
                WriteCacheManager.getCommonWriteCacheQueue()
                        .add(writeCacheBeingFlushed);
            } else {
                freeBlockCaches.add(writeCacheBeingFlushed);
            }

            double flushTimeSeconds = MathUtils.elapsedNanos(startTime)
                    / (double) TimeUnit.SECONDS.toNanos(1);
            double flushThroughput = sizeToFlush / 1024.0 / 1024.0 / flushTimeSeconds;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Flushing done time {} s -- Written {} MB/s",
                        flushTimeSeconds, flushThroughput);
            }

            recordSuccessfulEvent(dbLedgerStorageStats.getFlushStats(), startTime);
            dbLedgerStorageStats.getFlushSizeStats().registerSuccessfulValue(sizeToFlush);
            return true;
        } catch (IOException e) {
            recordFailedEvent(dbLedgerStorageStats.getFlushStats(), startTime);
            // Leave IOExecption as it is
            throw e;
        } catch (RuntimeException e) {
            recordFailedEvent(dbLedgerStorageStats.getFlushStats(), startTime);
            // Wrap unchecked exceptions
            throw new IOException(e);
        }
    }

    private void recordSuccessfulEvent(OpStatsLogger logger, long startTimeNanos) {
        logger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
    }

    private void recordFailedEvent(OpStatsLogger logger, long startTimeNanos) {
        logger.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
    }

    private class FlushWriteCacheThread extends BookieCriticalThread {

        volatile boolean running = true;

        public FlushWriteCacheThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            while (!disableFlushWriteCacheThread.get()
                    && (running || !fulledBlockCaches.isEmpty())) {
                long startTime = System.nanoTime();
                flushMutex.lock();
                try {
                    WriteCache writeCacheBeingFlushed = fulledBlockCaches.peek();
                    if (writeCacheBeingFlushed != null) {
                        CheckpointSource.Checkpoint checkpoint = writeCacheBeingFlushed.getCheckpoint();
                        long size = writeCacheBeingFlushed.size();
                        long count = writeCacheBeingFlushed.count();
                        if (checkpoint(writeCacheBeingFlushed)) {
                            checkpointSource.checkpointComplete(checkpoint, true);
                            fulledBlockCaches.remove(writeCacheBeingFlushed);
                            cacheSize.addAndGet(-1 * size);
                            cacheCount.add(-1 * count);
                            continue;
                        }
                    }
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    LOG.warn("throw InterruptedException in the FlushWriteCacheThread!", e);
                } catch (IOException e) {
                    LOG.error("Error during flush", e);
                } finally {
                    flushMutex.unlock();
                    flushExecutorTime.addLatency(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                }
            }
        }

        void shutdown() throws InterruptedException {
            running = false;
            this.join();
        }
    }

    public WriteCache pollFreeWriteCache() {
        return freeBlockCaches.poll();
    }


    public void flushAsync(WriteCache blockCache) {
        cacheSize.addAndGet(blockCache.size());
        cacheCount.add(blockCache.count());
        long stamp = rotationLock.readLock();
        try {
            fulledBlockCaches.offer(blockCache);
        } finally {
            rotationLock.unlockRead(stamp);
        }
    }

    public WriteCache flushAndPollFreeCache() {
        flushMutex.lock();
        try {
            flush();
            return freeBlockCaches.poll();
        } finally {
            flushMutex.unlock();
        }
    }

    public void flush() {
        LinkedBlockingDeque<WriteCache> fulledBlockCachesTmp;
        flushMutex.lock();
        try {
            long stamp = rotationLock.writeLock();
            try {
                fulledBlockCachesTmp = fulledBlockCaches;
                fulledBlockCaches = new LinkedBlockingDeque<>();
            } finally {
                rotationLock.unlockWrite(stamp);
            }

            while (!fulledBlockCachesTmp.isEmpty()) {
                WriteCache writeCacheBeingFlushed = fulledBlockCachesTmp.poll();
                long size = writeCacheBeingFlushed.size();
                long count = writeCacheBeingFlushed.count();
                long startTime = System.nanoTime();
                try {
                    Checkpoint checkpoint = writeCacheBeingFlushed.getCheckpoint();
                    checkpoint(writeCacheBeingFlushed);
                    checkpointSource.checkpointComplete(checkpoint, true);
                } catch (IOException e) {
                    LOG.error("Error during flush", e);
                } finally {
                    flushExecutorTime.addLatency(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
                    cacheCount.add(-1 * count);
                    cacheSize.addAndGet(-1 * size);
                }
            }
        } finally {
            flushMutex.unlock();
        }
    }

    public void shutdown() throws InterruptedException {
        flushWriteCacheThread.shutdown();
        cleanupExecutor.shutdown();
        while (!freeBlockCaches.isEmpty()) {
            WriteCache cache = freeBlockCaches.poll();
            cache.close();
        }
    }
}
