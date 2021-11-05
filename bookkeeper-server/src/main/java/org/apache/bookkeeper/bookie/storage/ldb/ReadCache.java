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

import static org.apache.bookkeeper.bookie.storage.ldb.WriteCache.align64;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongLongPairHashMap.LongPair;

/**
 * Read cache implementation.
 *
 * <p>Uses the specified amount of memory and pairs it with a hashmap.
 *
 * <p>The memory is splitted in multiple segments that are used in a
 * ring-buffer fashion. When the read cache is full, the oldest segment
 * is cleared and rotated to make space for new entries to be added to
 * the read cache.
 */
public class ReadCache implements Closeable {

    private static final int DEFAULT_MAX_SEGMENT_SIZE = 1 * 1024 * 1024 * 1024;

    private final List<ByteBuf> cacheSegments;
    private final List<ConcurrentLongLongPairHashMap> cacheIndexes;

    private int currentSegmentIdx;
    private final AtomicInteger currentSegmentOffset = new AtomicInteger(0);

    private final int segmentSize;

    private ByteBufAllocator allocator;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public ReadCache(ByteBufAllocator allocator, long maxCacheSize) {
        this(allocator, maxCacheSize, DEFAULT_MAX_SEGMENT_SIZE);
    }

    public ReadCache(ByteBufAllocator allocator, long maxCacheSize, int maxSegmentSize) {
        this.allocator = allocator;
        int segmentsCount = Math.max(2, (int) (maxCacheSize / maxSegmentSize));
        segmentSize = (int) (maxCacheSize / segmentsCount);

        cacheSegments = new ArrayList<>();
        cacheIndexes = new ArrayList<>();

        for (int i = 0; i < segmentsCount; i++) {
            cacheSegments.add(Unpooled.directBuffer(segmentSize, segmentSize));
            cacheIndexes.add(new ConcurrentLongLongPairHashMap(4096, 2 * Runtime.getRuntime().availableProcessors()));
        }
    }

    @Override
    public void close() {
        cacheSegments.forEach(ByteBuf::release);
    }

    public void put(long ledgerId, long entryId, ByteBuf entry) {
        int entrySize = entry.readableBytes();
        int alignedSize = align64(entrySize);

        lock.readLock().lock();

        try {
            int offset = currentSegmentOffset.getAndAdd(alignedSize);
            if (offset + entrySize > segmentSize) {
                // Roll-over the segment (outside the read-lock)
            } else {
                // Copy entry into read cache segment
                cacheSegments.get(currentSegmentIdx).setBytes(offset, entry, entry.readerIndex(),
                        entry.readableBytes());
                cacheIndexes.get(currentSegmentIdx).put(ledgerId, entryId, offset, entrySize);
                return;
            }
        } finally {
            lock.readLock().unlock();
        }

        // We could not insert in segment, we to get the write lock and roll-over to
        // next segment
        lock.writeLock().lock();

        try {
            int offset = currentSegmentOffset.getAndAdd(entrySize);
            if (offset + entrySize > segmentSize) {
                // Rollover to next segment
                currentSegmentIdx = (currentSegmentIdx + 1) % cacheSegments.size();
                currentSegmentOffset.set(alignedSize);
                cacheIndexes.get(currentSegmentIdx).clear();
                offset = 0;
            }

            // Copy entry into read cache segment
            cacheSegments.get(currentSegmentIdx).setBytes(offset, entry, entry.readerIndex(), entry.readableBytes());
            cacheIndexes.get(currentSegmentIdx).put(ledgerId, entryId, offset, entrySize);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public ByteBuf get(long ledgerId, long entryId) {
        lock.readLock().lock();

        try {
            // We need to check all the segments, starting from the current one and looking
            // backward to minimize the
            // checks for recently inserted entries
            int size = cacheSegments.size();
            for (int i = 0; i < size; i++) {
                int segmentIdx = (currentSegmentIdx + (size - i)) % size;

                LongPair res = cacheIndexes.get(segmentIdx).get(ledgerId, entryId);
                if (res != null) {
                    int entryOffset = (int) res.first;
                    int entryLen = (int) res.second;

                    ByteBuf entry = allocator.directBuffer(entryLen, entryLen);
                    entry.writeBytes(cacheSegments.get(segmentIdx), entryOffset, entryLen);
                    return entry;
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        // Entry not found in any segment
        return null;
    }

    /**
     * @return the total size of cached entries
     */
    public long size() {
        lock.readLock().lock();

        try {
            long size = 0;
            for (int i = 0; i < cacheIndexes.size(); i++) {
                if (i == currentSegmentIdx) {
                    size += currentSegmentOffset.get();
                } else if (!cacheIndexes.get(i).isEmpty()) {
                    size += segmentSize;
                } else {
                    // the segment is empty
                }
            }

            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @return the total number of cached entries
     */
    public long count() {
        lock.readLock().lock();

        try {
            long count = 0;
            for (int i = 0; i < cacheIndexes.size(); i++) {
                count += cacheIndexes.get(i).size();
            }

            return count;
        } finally {
            lock.readLock().unlock();
        }
    }
}
