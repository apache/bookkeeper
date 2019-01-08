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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashSet;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap.LongPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write cache implementation.
 *
 * <p>The write cache will allocate the requested size from direct memory and it
 * will break it down into multiple segments.
 *
 * <p>The entries are appended in a common buffer and indexed though a hashmap,
 * until the cache is cleared.
 *
 * <p>There is the possibility to iterate through the stored entries in an ordered
 * way, by (ledgerId, entry).
 */
public class WriteCache implements Closeable {

    /**
     * Consumer that is used to scan the entire write cache.
     */
    public interface EntryConsumer {
        void accept(long ledgerId, long entryId, ByteBuf entry);
    }

    private final ConcurrentLongLongPairHashMap index =
            new ConcurrentLongLongPairHashMap(4096, 2 * Runtime.getRuntime().availableProcessors());

    private final ConcurrentLongLongHashMap lastEntryMap =
            new ConcurrentLongLongHashMap(4096, 2 * Runtime.getRuntime().availableProcessors());

    private final ByteBuf[] cacheSegments;
    private final int segmentsCount;

    private final long maxCacheSize;
    private final int maxSegmentSize;
    private final long segmentOffsetMask;
    private final long segmentOffsetBits;

    private final AtomicLong cacheSize = new AtomicLong(0);
    private final AtomicLong cacheOffset = new AtomicLong(0);
    private final LongAdder cacheCount = new LongAdder();

    private final ConcurrentLongHashSet deletedLedgers = new ConcurrentLongHashSet();

    private final ByteBufAllocator allocator;

    public WriteCache(ByteBufAllocator allocator, long maxCacheSize) {
        // Default maxSegmentSize set to 1Gb
        this(allocator, maxCacheSize, 1 * 1024 * 1024 * 1024);
    }

    public WriteCache(ByteBufAllocator allocator, long maxCacheSize, int maxSegmentSize) {
        checkArgument(maxSegmentSize > 0);

        long alignedMaxSegmentSize = alignToPowerOfTwo(maxSegmentSize);
        checkArgument(maxSegmentSize == alignedMaxSegmentSize, "Max segment size needs to be in form of 2^n");

        this.allocator = allocator;
        this.maxCacheSize = maxCacheSize;
        this.maxSegmentSize = (int) maxSegmentSize;
        this.segmentOffsetMask = maxSegmentSize - 1;
        this.segmentOffsetBits = 63 - Long.numberOfLeadingZeros(maxSegmentSize);

        this.segmentsCount = 1 + (int) (maxCacheSize / maxSegmentSize);

        this.cacheSegments = new ByteBuf[segmentsCount];

        for (int i = 0; i < segmentsCount - 1; i++) {
            // All intermediate segments will be full-size
            cacheSegments[i] = Unpooled.directBuffer(maxSegmentSize, maxSegmentSize);
        }

        int lastSegmentSize = (int) (maxCacheSize % maxSegmentSize);
        cacheSegments[segmentsCount - 1] = Unpooled.directBuffer(lastSegmentSize, lastSegmentSize);
    }

    public void clear() {
        cacheSize.set(0L);
        cacheOffset.set(0L);
        cacheCount.reset();
        index.clear();
        lastEntryMap.clear();
        deletedLedgers.clear();
    }

    @Override
    public void close() {
        for (ByteBuf buf : cacheSegments) {
            buf.release();
        }
    }

    public boolean put(long ledgerId, long entryId, ByteBuf entry) {
        int size = entry.readableBytes();

        // Align to 64 bytes so that different threads will not contend the same L1
        // cache line
        int alignedSize = align64(size);

        long offset;
        int localOffset;
        int segmentIdx;

        while (true) {
            offset = cacheOffset.getAndAdd(alignedSize);
            localOffset = (int) (offset & segmentOffsetMask);
            segmentIdx = (int) (offset >>> segmentOffsetBits);

            if ((offset + size) > maxCacheSize) {
                // Cache is full
                return false;
            } else if (maxSegmentSize - localOffset < size) {
                // If an entry is at the end of a segment, we need to get a new offset and try
                // again in next segment
                continue;
            } else {
                // Found a good offset
                break;
            }
        }

        cacheSegments[segmentIdx].setBytes(localOffset, entry, entry.readerIndex(), entry.readableBytes());

        // Update last entryId for ledger. This logic is to handle writes for the same
        // ledger coming out of order and from different thread, though in practice it
        // should not happen and the compareAndSet should be always uncontended.
        while (true) {
            long currentLastEntryId = lastEntryMap.get(ledgerId);
            if (currentLastEntryId > entryId) {
                // A newer entry is already there
                break;
            }

            if (lastEntryMap.compareAndSet(ledgerId, currentLastEntryId, entryId)) {
                break;
            }
        }

        index.put(ledgerId, entryId, offset, size);
        cacheCount.increment();
        cacheSize.addAndGet(size);
        return true;
    }

    public ByteBuf get(long ledgerId, long entryId) {
        LongPair result = index.get(ledgerId, entryId);
        if (result == null) {
            return null;
        }

        long offset = result.first;
        int size = (int) result.second;
        ByteBuf entry = allocator.buffer(size, size);

        int localOffset = (int) (offset & segmentOffsetMask);
        int segmentIdx = (int) (offset >>> segmentOffsetBits);
        entry.writeBytes(cacheSegments[segmentIdx], localOffset, size);
        return entry;
    }

    public ByteBuf getLastEntry(long ledgerId) {
        long lastEntryId = lastEntryMap.get(ledgerId);
        if (lastEntryId == -1) {
            // Ledger not found in write cache
            return null;
        } else {
            return get(ledgerId, lastEntryId);
        }
    }

    public void deleteLedger(long ledgerId) {
        deletedLedgers.add(ledgerId);
    }

    private static final ArrayGroupSort groupSorter = new ArrayGroupSort(2, 4);

    public void forEach(EntryConsumer consumer) {
        sortedEntriesLock.lock();

        try {
            int entriesToSort = (int) index.size();
            int arrayLen = entriesToSort * 4;
            if (sortedEntries == null || sortedEntries.length < arrayLen) {
                sortedEntries = new long[(int) (arrayLen * 2)];
            }

            long startTime = MathUtils.nowInNano();

            sortedEntriesIdx = 0;
            index.forEach((ledgerId, entryId, offset, length) -> {
                if (deletedLedgers.contains(ledgerId)) {
                    // Ignore deleted ledgers
                    return;
                }

                sortedEntries[sortedEntriesIdx] = ledgerId;
                sortedEntries[sortedEntriesIdx + 1] = entryId;
                sortedEntries[sortedEntriesIdx + 2] = offset;
                sortedEntries[sortedEntriesIdx + 3] = length;
                sortedEntriesIdx += 4;
            });

            if (log.isDebugEnabled()) {
                log.debug("iteration took {} ms", MathUtils.elapsedNanos(startTime) / 1e6);
            }
            startTime = MathUtils.nowInNano();

            // Sort entries by (ledgerId, entryId) maintaining the 4 items groups
            groupSorter.sort(sortedEntries, 0, sortedEntriesIdx);
            if (log.isDebugEnabled()) {
                log.debug("sorting {} ms", (MathUtils.elapsedNanos(startTime) / 1e6));
            }
            startTime = MathUtils.nowInNano();

            ByteBuf[] entrySegments = new ByteBuf[segmentsCount];
            for (int i = 0; i < segmentsCount; i++) {
                entrySegments[i] = cacheSegments[i].slice(0, cacheSegments[i].capacity());
            }

            for (int i = 0; i < sortedEntriesIdx; i += 4) {
                long ledgerId = sortedEntries[i];
                long entryId = sortedEntries[i + 1];
                long offset = sortedEntries[i + 2];
                long length = sortedEntries[i + 3];

                int localOffset = (int) (offset & segmentOffsetMask);
                int segmentIdx = (int) (offset >>> segmentOffsetBits);
                ByteBuf entry = entrySegments[segmentIdx];
                entry.setIndex(localOffset, localOffset + (int) length);
                consumer.accept(ledgerId, entryId, entry);
            }

            if (log.isDebugEnabled()) {
                log.debug("entry log adding {} ms", MathUtils.elapsedNanos(startTime) / 1e6);
            }
        } finally {
            sortedEntriesLock.unlock();
        }
    }

    public long size() {
        return cacheSize.get();
    }

    public long count() {
        return cacheCount.sum();
    }

    public boolean isEmpty() {
        return cacheSize.get() == 0L;
    }

    private static final int ALIGN_64_MASK = ~(64 - 1);

    static int align64(int size) {
        return (size + 64 - 1) & ALIGN_64_MASK;
    }

    private static long alignToPowerOfTwo(long n) {
        return (long) Math.pow(2, 64 - Long.numberOfLeadingZeros(n - 1));
    }

    private final ReentrantLock sortedEntriesLock = new ReentrantLock();
    private long[] sortedEntries;
    private int sortedEntriesIdx;

    private static final Logger log = LoggerFactory.getLogger(WriteCache.class);
}
