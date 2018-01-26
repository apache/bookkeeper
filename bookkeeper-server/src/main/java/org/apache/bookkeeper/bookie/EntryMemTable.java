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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SKIP_LIST_FLUSH_BYTES;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SKIP_LIST_GET_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SKIP_LIST_PUT_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SKIP_LIST_SNAPSHOT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.SKIP_LIST_THROTTLING;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The EntryMemTable holds in-memory representation to the entries not-yet flushed.
 * When asked to flush, current EntrySkipList is moved to snapshot and is cleared.
 * We continue to serve edits out of new EntrySkipList and backing snapshot until
 * flusher reports in that the flush succeeded. At that point we let the snapshot go.
 */
public class EntryMemTable {
    private static Logger logger = LoggerFactory.getLogger(EntryMemTable.class);

    /**
     * Entry skip list.
     */
    static class EntrySkipList extends ConcurrentSkipListMap<EntryKey, EntryKeyValue> {
        final Checkpoint cp;
        static final EntrySkipList EMPTY_VALUE = new EntrySkipList(Checkpoint.MAX) {
            @Override
            public boolean isEmpty() {
                return true;
            }
        };

        EntrySkipList(final Checkpoint cp) {
            super(EntryKey.COMPARATOR);
            this.cp = cp;
        }

        int compareTo(final Checkpoint cp) {
            return this.cp.compareTo(cp);
        }

        @Override
        public EntryKeyValue put(EntryKey k, EntryKeyValue v) {
            return putIfAbsent(k, v);
        }

        @Override
        public EntryKeyValue putIfAbsent(EntryKey k, EntryKeyValue v) {
            assert k.equals(v);
            return super.putIfAbsent(v, v);
        }

        @Override
        public boolean equals(Object o) {
            return this == o;
        }
    }

    volatile EntrySkipList kvmap;

    // Snapshot of EntryMemTable.  Made for flusher.
    volatile EntrySkipList snapshot;

    final ServerConfiguration conf;
    final CheckpointSource checkpointSource;

    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Used to track own data size
    final AtomicLong size;

    final long skipListSizeLimit;

    SkipListArena allocator;

    // flag indicating the status of the previous flush call
    private final AtomicBoolean previousFlushSucceeded;

    private EntrySkipList newSkipList() {
        return new EntrySkipList(checkpointSource.newCheckpoint());
    }

    private final OrderedSafeExecutor flushExecutor;
    private final int memtableFlushTimeoutInSeconds;

    // Stats
    private final OpStatsLogger snapshotStats;
    private final OpStatsLogger putEntryStats;
    private final OpStatsLogger getEntryStats;
    private final Counter flushBytesCounter;
    private final Counter throttlingCounter;

    /**
    * Constructor.
    * @param conf Server configuration
    */
    public EntryMemTable(final ServerConfiguration conf, final CheckpointSource source,
            OrderedSafeExecutor flushExecutor, final StatsLogger statsLogger) {
        this.checkpointSource = source;
        this.kvmap = newSkipList();
        this.snapshot = EntrySkipList.EMPTY_VALUE;
        this.conf = conf;
        this.size = new AtomicLong(0);
        this.allocator = new SkipListArena(conf);
        this.previousFlushSucceeded = new AtomicBoolean(true);
        // skip list size limit
        this.skipListSizeLimit = conf.getSkipListSizeLimit();

        // Stats
        this.snapshotStats = statsLogger.getOpStatsLogger(SKIP_LIST_SNAPSHOT);
        this.putEntryStats = statsLogger.getOpStatsLogger(SKIP_LIST_PUT_ENTRY);
        this.getEntryStats = statsLogger.getOpStatsLogger(SKIP_LIST_GET_ENTRY);
        this.flushBytesCounter = statsLogger.getCounter(SKIP_LIST_FLUSH_BYTES);
        this.throttlingCounter = statsLogger.getCounter(SKIP_LIST_THROTTLING);
        this.flushExecutor = flushExecutor;
        this.memtableFlushTimeoutInSeconds = conf.getMemtableFlushTimeoutInSeconds();
    }

    void dump() {
        for (EntryKey key: this.kvmap.keySet()) {
            logger.info(key.toString());
        }
        for (EntryKey key: this.snapshot.keySet()) {
            logger.info(key.toString());
        }
    }

    Checkpoint snapshot() throws IOException {
        return snapshot(Checkpoint.MAX);
    }

    /**
     * Snapshot current EntryMemTable. if given <i>oldCp</i> is older than current checkpoint,
     * we don't do any snapshot. If snapshot happened, we return the checkpoint of the snapshot.
     *
     * @param oldCp
     *          checkpoint
     * @return checkpoint of the snapshot, null means no snapshot
     * @throws IOException
     */
    Checkpoint snapshot(Checkpoint oldCp) throws IOException {
        Checkpoint cp = null;
        // No-op if snapshot currently has entries
        if (this.snapshot.isEmpty() && this.kvmap.compareTo(oldCp) < 0) {
            final long startTimeNanos = MathUtils.nowInNano();
            this.lock.writeLock().lock();
            try {
                if (this.snapshot.isEmpty() && !this.kvmap.isEmpty()
                        && this.kvmap.compareTo(oldCp) < 0) {
                    this.snapshot = this.kvmap;
                    this.kvmap = newSkipList();
                    // get the checkpoint of the memtable.
                    cp = this.kvmap.cp;
                    // Reset heap to not include any keys
                    this.size.set(0);
                    // Reset allocator so we get a fresh buffer for the new EntryMemTable
                    this.allocator = new SkipListArena(conf);
                }
            } finally {
                this.lock.writeLock().unlock();
            }

            if (null != cp) {
                snapshotStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            } else {
                snapshotStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            }
        }
        return cp;
    }

    /**
     * Flush snapshot and clear it.
     */
    long flush(final SkipListFlusher flusher) throws IOException {
        try {
            long flushSize = flushSnapshot(flusher, Checkpoint.MAX);
            previousFlushSucceeded.set(true);
            return flushSize;
        } catch (IOException ioe) {
            previousFlushSucceeded.set(false);
            throw ioe;
        }
    }

    /**
     * Flush memtable until checkpoint.
     *
     * @param checkpoint
     *          all data before this checkpoint need to be flushed.
     */
    public long flush(SkipListFlusher flusher, Checkpoint checkpoint) throws IOException {
        try {
            long size = flushSnapshot(flusher, checkpoint);
            if (null != snapshot(checkpoint)) {
                size += flushSnapshot(flusher, checkpoint);
            }
            previousFlushSucceeded.set(true);
            return size;
        } catch (IOException ioe) {
            previousFlushSucceeded.set(false);
            throw ioe;
        }
    }

    /**
     * Flush snapshot and clear it iff its data is before checkpoint.
     * Only this function change non-empty this.snapshot.
     */
    private long flushSnapshot(final SkipListFlusher flusher, Checkpoint checkpoint) throws IOException {
        if (flushExecutor == null) {
            return flushSnapshotSequentially(flusher, checkpoint);
        } else {
            return flushSnapshotParallelly(flusher, checkpoint);
        }
    }

    long flushSnapshotSequentially(final SkipListFlusher flusher, Checkpoint checkpoint) throws IOException {
        long size = 0;
        if (this.snapshot.compareTo(checkpoint) < 0) {
            long ledger, ledgerGC = -1;
            synchronized (this) {
                EntrySkipList keyValues = this.snapshot;
                if (keyValues.compareTo(checkpoint) < 0) {
                    for (EntryKey key : keyValues.keySet()) {
                        EntryKeyValue kv = (EntryKeyValue) key;
                        size += kv.getLength();
                        ledger = kv.getLedgerId();
                        if (ledgerGC != ledger) {
                            try {
                                flusher.process(ledger, kv.getEntryId(), kv.getValueAsByteBuffer());
                            } catch (NoLedgerException exception) {
                                ledgerGC = ledger;
                            }
                        }
                    }
                    flushBytesCounter.add(size);
                    clearSnapshot(keyValues);
                }
            }
        }

        return size;
    }

    long flushSnapshotParallelly(final SkipListFlusher flusher, Checkpoint checkpoint) throws IOException {
        AtomicLong flushedSize = new AtomicLong();
        if (this.snapshot.compareTo(checkpoint) < 0) {
            synchronized (this) {
                EntrySkipList keyValues = this.snapshot;
                if (keyValues.compareTo(checkpoint) < 0) {
                    NavigableSet<EntryKey> keyValuesSet = keyValues.keySet();
                    Map<Long, List<EntryKeyValue>> entryKeyValuesMap = new HashMap<Long, List<EntryKeyValue>>();

                    for (EntryKey key : keyValuesSet) {
                        EntryKeyValue kv = (EntryKeyValue) key;
                        Long ledger = kv.getLedgerId();
                        if (!entryKeyValuesMap.containsKey(ledger)) {
                            entryKeyValuesMap.put(ledger, new LinkedList<EntryKeyValue>());
                        }
                        entryKeyValuesMap.get(ledger).add(kv);
                    }

                    CountDownLatch latch = new CountDownLatch(entryKeyValuesMap.size());
                    AtomicBoolean isFlushThreadInterrupted = new AtomicBoolean(false);
                    AtomicReference<Exception> exceptionWhileFlushingParallelly =  new AtomicReference<Exception>();
                    Thread mainFlushThread = Thread.currentThread();

                    for (Long ledgerId : entryKeyValuesMap.keySet()) {
                        List<EntryKeyValue> entryKeyValuesOfALedger = entryKeyValuesMap.get(ledgerId);
                        flushExecutor.submitOrdered(ledgerId, new SafeRunnable() {
                            @Override
                            public void safeRun() {
                                for (EntryKeyValue entryKeyValue : entryKeyValuesOfALedger) {
                                    try {
                                        flusher.process(ledgerId, entryKeyValue.getEntryId(),
                                                entryKeyValue.getValueAsByteBuffer());
                                        flushedSize.addAndGet(entryKeyValue.getLength());
                                    } catch (NoLedgerException exception) {
                                        logger.info("Got NoLedgerException while flushing entry: {}. The ledger "
                                                + "must be deleted " + "after this entry is added to the Memtable",
                                                entryKeyValue);
                                        break;
                                    } catch (Exception exc) {
                                        logger.error(
                                                "Got Exception while trying to flush process entry: " + entryKeyValue,
                                                exc);
                                        if (isFlushThreadInterrupted.compareAndSet(false, true)) {
                                            exceptionWhileFlushingParallelly.set(exc);
                                            mainFlushThread.interrupt();
                                        }
                                        // return without countdowning the latch since we got unexpected Exception
                                        return;
                                    }
                                }
                                latch.countDown();
                            }
                        });
                    }

                    try {
                        while (!latch.await(memtableFlushTimeoutInSeconds, TimeUnit.SECONDS)) {
                            logger.error("Entrymemtable parallel flush has not completed in {0} secs, so waiting again",
                                    memtableFlushTimeoutInSeconds);
                        }
                        assert (exceptionWhileFlushingParallelly.get() == null);
                        flushBytesCounter.add(flushedSize.get());
                        clearSnapshot(keyValues);
                    } catch (InterruptedException ie) {
                        logger.error("Got Interrupted exception while waiting for the flushexecutor "
                                + "to complete the entry flushes");
                        throw new IOException("Failed to complete the flushSnapshotByParallelizing",
                                exceptionWhileFlushingParallelly.get());
                    }
                }
            }
        }
        return flushedSize.longValue();
    }

    /**
     * The passed snapshot was successfully persisted; it can be let go.
     * @param keyValues The snapshot to clean out.
     * @see {@link #snapshot()}
     */
    private void clearSnapshot(final EntrySkipList keyValues) {
        // Caller makes sure that keyValues not empty
        assert !keyValues.isEmpty();
        this.lock.writeLock().lock();
        try {
            // create a new snapshot and let the old one go.
            assert this.snapshot == keyValues;
            this.snapshot = EntrySkipList.EMPTY_VALUE;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * Throttling writer w/ 1 ms delay.
     */
    private void throttleWriters() {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        throttlingCounter.inc();
    }

    /**
     * Write an update.
     *
     * @param entry
     * @return approximate size of the passed key and value.
     * @throws IOException
     */
    public long addEntry(long ledgerId, long entryId, final ByteBuffer entry, final CacheCallback cb)
            throws IOException {
        long size = 0;
        long startTimeNanos = MathUtils.nowInNano();
        boolean success = false;
        try {
            if (isSizeLimitReached() || (!previousFlushSucceeded.get())) {
                Checkpoint cp = snapshot();
                if ((null != cp) || (!previousFlushSucceeded.get())) {
                    cb.onSizeLimitReached(cp);
                } else {
                    throttleWriters();
                }
            }

            this.lock.readLock().lock();
            try {
                EntryKeyValue toAdd = cloneWithAllocator(ledgerId, entryId, entry);
                size = internalAdd(toAdd);
            } finally {
                this.lock.readLock().unlock();
            }
            success = true;
            return size;
        } finally {
            if (success) {
                putEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            } else {
                putEntryStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            }
        }
    }

    /**
    * Internal version of add() that doesn't clone KVs with the
    * allocator, and doesn't take the lock.
    * Callers should ensure they already have the read lock taken
    */
    private long internalAdd(final EntryKeyValue toAdd) throws IOException {
        long sizeChange = 0;
        if (kvmap.putIfAbsent(toAdd, toAdd) == null) {
            sizeChange = toAdd.getLength();
            size.addAndGet(sizeChange);
        }
        return sizeChange;
    }

    private EntryKeyValue newEntry(long ledgerId, long entryId, final ByteBuffer entry) {
        byte[] buf;
        int offset = 0;
        int length = entry.remaining();

        if (entry.hasArray()) {
            buf = entry.array();
            offset = entry.arrayOffset();
        } else {
            buf = new byte[length];
            entry.get(buf);
        }
        return new EntryKeyValue(ledgerId, entryId, buf, offset, length);
    }

    private EntryKeyValue cloneWithAllocator(long ledgerId, long entryId, final ByteBuffer entry) {
        int len = entry.remaining();
        SkipListArena.MemorySlice alloc = allocator.allocateBytes(len);
        if (alloc == null) {
            // The allocation was too large, allocator decided
            // not to do anything with it.
            return newEntry(ledgerId, entryId, entry);
        }

        assert alloc.getData() != null;
        entry.get(alloc.getData(), alloc.getOffset(), len);
        return new EntryKeyValue(ledgerId, entryId, alloc.getData(), alloc.getOffset(), len);
    }

    /**
     * Find the entry with given key.
     * @param ledgerId
     * @param entryId
     * @return the entry kv or null if none found.
     */
    public EntryKeyValue getEntry(long ledgerId, long entryId) throws IOException {
        EntryKey key = new EntryKey(ledgerId, entryId);
        EntryKeyValue value = null;
        long startTimeNanos = MathUtils.nowInNano();
        boolean success = false;
        this.lock.readLock().lock();
        try {
            value = this.kvmap.get(key);
            if (value == null) {
                value = this.snapshot.get(key);
            }
            success = true;
        } finally {
            this.lock.readLock().unlock();
            if (success) {
                getEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            } else {
                getEntryStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            }
        }

        return value;
    }

    /**
     * Find the last entry with the given ledger key.
     * @param ledgerId
     * @return the entry kv or null if none found.
     */
    public EntryKeyValue getLastEntry(long ledgerId) throws IOException {
        EntryKey result = null;
        EntryKey key = new EntryKey(ledgerId, Long.MAX_VALUE);
        long startTimeNanos = MathUtils.nowInNano();
        boolean success = false;
        this.lock.readLock().lock();
        try {
            result = this.kvmap.floorKey(key);
            if (result == null || result.getLedgerId() != ledgerId) {
                result = this.snapshot.floorKey(key);
            }
            success = true;
        } finally {
            this.lock.readLock().unlock();
            if (success) {
                getEntryStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            } else {
                getEntryStats.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            }
        }

        if (result == null || result.getLedgerId() != ledgerId) {
            return null;
        }
        return (EntryKeyValue) result;
    }

    /**
     * Check if the entire heap usage for this EntryMemTable exceeds limit.
     */
    boolean isSizeLimitReached() {
        return size.get() >= skipListSizeLimit;
    }

    /**
     * Check if there is data in the mem-table.
     * @return
     */
    boolean isEmpty() {
        return size.get() == 0 && snapshot.isEmpty();
    }
}
