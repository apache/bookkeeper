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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.stats.EntryMemTableStats;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.IteratorUtility;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The EntryMemTable holds in-memory representation to the entries not-yet flushed.
 * When asked to flush, current EntrySkipList is moved to snapshot and is cleared.
 * We continue to serve edits out of new EntrySkipList and backing snapshot until
 * flusher reports in that the flush succeeded. At that point we let the snapshot go.
 */
public class EntryMemTable implements AutoCloseable{
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
    final Semaphore skipListSemaphore;

    SkipListArena allocator;

    // flag indicating the status of the previous flush call
    private final AtomicBoolean previousFlushSucceeded;

    private EntrySkipList newSkipList() {
        return new EntrySkipList(checkpointSource.newCheckpoint());
    }

    // Stats
    protected final EntryMemTableStats memTableStats;

    /**
    * Constructor.
    * @param conf Server configuration
    */
    public EntryMemTable(final ServerConfiguration conf, final CheckpointSource source,
                         final StatsLogger statsLogger) {
        this.checkpointSource = source;
        this.kvmap = newSkipList();
        this.snapshot = EntrySkipList.EMPTY_VALUE;
        this.conf = conf;
        this.size = new AtomicLong(0);
        this.allocator = new SkipListArena(conf);
        this.previousFlushSucceeded = new AtomicBoolean(true);
        // skip list size limit
        this.skipListSizeLimit = conf.getSkipListSizeLimit();

        if (skipListSizeLimit > (Integer.MAX_VALUE - 1) / 2) {
            // gives 2*1023MB for mem table.
            // consider a way to create semaphore with long num of permits
            // until that 1023MB should be enough for everything (tm)
            throw new IllegalArgumentException("skiplist size over " + ((Integer.MAX_VALUE - 1) / 2));
        }
        // double the size for snapshot in progress + incoming data
        this.skipListSemaphore = new Semaphore((int) skipListSizeLimit * 2);

        // Stats
        this.memTableStats = new EntryMemTableStats(statsLogger);
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
                memTableStats.getSnapshotStats()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            } else {
                memTableStats.getSnapshotStats()
                    .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
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
     * Flush snapshot and clear it iff its data is before checkpoint. Only this
     * function change non-empty this.snapshot.
     *
     * <p>EntryMemTableWithParallelFlusher overrides this flushSnapshot method. So
     * any change in functionality/behavior/characteristic of this method should
     * also reflect in EntryMemTableWithParallelFlusher's flushSnapshot method.
     */
    long flushSnapshot(final SkipListFlusher flusher, Checkpoint checkpoint) throws IOException {
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
                    memTableStats.getFlushBytesCounter().add(size);
                    clearSnapshot(keyValues);
                }
            }
        }

        skipListSemaphore.release((int) size);
        return size;
    }

    /**
     * The passed snapshot was successfully persisted; it can be let go.
     * @param keyValues The snapshot to clean out.
     * @see {@link #snapshot()}
     */
    void clearSnapshot(final EntrySkipList keyValues) {
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
                }
            }

            final int len = entry.remaining();
            if (!skipListSemaphore.tryAcquire(len)) {
                memTableStats.getThrottlingCounter().inc();
                final long throttlingStartTimeNanos = MathUtils.nowInNano();
                skipListSemaphore.acquireUninterruptibly(len);
                memTableStats.getThrottlingStats()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(throttlingStartTimeNanos), TimeUnit.NANOSECONDS);
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
                memTableStats.getPutEntryStats()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            } else {
                memTableStats.getPutEntryStats()
                    .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
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

        buf = new byte[length];
        entry.get(buf);
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
                memTableStats.getGetEntryStats()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            } else {
                memTableStats.getGetEntryStats()
                    .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
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
                memTableStats.getGetEntryStats()
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
            } else {
                memTableStats.getGetEntryStats()
                    .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
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

    @Override
    public void close() throws Exception {
        // no-op
    }

    /*
     * returns the primitive long iterator of entries of a ledger available in
     * this EntryMemTable. It would be in the ascending order and this Iterator
     * is weakly consistent.
     */
    PrimitiveIterator.OfLong getListOfEntriesOfLedger(long ledgerId) {
        EntryKey thisLedgerFloorEntry = new EntryKey(ledgerId, 0);
        EntryKey thisLedgerCeilingEntry = new EntryKey(ledgerId, Long.MAX_VALUE);
        Iterator<EntryKey> thisLedgerEntriesInKVMap;
        Iterator<EntryKey> thisLedgerEntriesInSnapshot;
        this.lock.readLock().lock();
        try {
            /*
             * Gets a view of the portion of this map that corresponds to
             * entries of this ledger.
             *
             * Here 'kvmap' is of type 'ConcurrentSkipListMap', so its 'subMap'
             * call would return a view of the portion of this map whose keys
             * range from fromKey to toKey and it would be of type
             * 'ConcurrentNavigableMap'. ConcurrentNavigableMap's 'keySet' would
             * return NavigableSet view of the keys contained in this map. This
             * view's iterator would be weakly consistent -
             * https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/
             * package-summary.html#Weakly.
             *
             * 'weakly consistent' would guarantee 'to traverse elements as they
             * existed upon construction exactly once, and may (but are not
             * guaranteed to) reflect any modifications subsequent to
             * construction.'
             *
             */
            thisLedgerEntriesInKVMap = this.kvmap.subMap(thisLedgerFloorEntry, thisLedgerCeilingEntry).keySet()
                    .iterator();
            thisLedgerEntriesInSnapshot = this.snapshot.subMap(thisLedgerFloorEntry, thisLedgerCeilingEntry).keySet()
                    .iterator();
        } finally {
            this.lock.readLock().unlock();
        }
        return IteratorUtility.mergeIteratorsForPrimitiveLongIterator(thisLedgerEntriesInKVMap,
                thisLedgerEntriesInSnapshot, EntryKey.COMPARATOR, (entryKey) -> {
                    return entryKey.entryId;
                });
    }
}
