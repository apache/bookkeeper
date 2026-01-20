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

import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Location cache.
 *
 * <p> This class is used to cache the location of entries in the entry logger.
 *
 * <p> The location of an entry is the position of the entry in the entry logger.
 */

/**
 * LocationCache with composite key (ledgerId | entryId) using SkipList
 */
public class LocationCache {
    private final static int MAX_ENTRIES = 1000000;
    private static final long TIME_WINDOW_MS = 100;
    private final NavigableMap<CompositeKey, Long> skipMap = new ConcurrentSkipListMap<>();
    private final long maxEntries;
    private final NavigableMap<Long, Set<Long>> createTime2LedgerIds = new ConcurrentSkipListMap<>();
    private final ConcurrentHashMap<Long, Long> ledgerId2CreateTime = new ConcurrentHashMap<>();
    private final ThreadLocal<CompositeKey> queryKey =
            ThreadLocal.withInitial(() -> new CompositeKey(0, 0));
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    static class CompositeKey implements Comparable<CompositeKey> {
        long ledgerId;
        long entryId;

        CompositeKey(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        void set(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        @Override
        public int compareTo(CompositeKey other) {
            int cmp = Long.compare(this.ledgerId, other.ledgerId);
            if (cmp != 0) {
                return cmp;
            }
            return Long.compare(this.entryId, other.entryId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CompositeKey that = (CompositeKey) o;
            return ledgerId == that.ledgerId && entryId == that.entryId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ledgerId, entryId);
        }
    }

    public LocationCache() {
        this(MAX_ENTRIES);
    }

    public LocationCache(long maxEntries) {
        this.maxEntries = maxEntries;
    }

    public long getIfExists(long ledgerId, long entryId) {
        CompositeKey key = queryKey.get();
        key.set(ledgerId, entryId);
        Long position = skipMap.get(key);
        return position != null ? position : 0L;
    }

    public void put(long ledgerId, long entryId, long position) {
        CompositeKey key = new CompositeKey(ledgerId, entryId);
        if (skipMap.containsKey(key)) {
            skipMap.put(key, position);
            return;
        }
        if (skipMap.size() >= maxEntries) {
            evictOldestEntries();
        }

        lock.readLock().lock();
        try {
            skipMap.put(key, position);
            Long timeStamp = ledgerId2CreateTime.computeIfAbsent(ledgerId,
                    __ -> System.currentTimeMillis() / TIME_WINDOW_MS);
            createTime2LedgerIds.computeIfAbsent(timeStamp,
                    __ -> ConcurrentHashMap.newKeySet()).add(ledgerId);
        } finally {
            lock.readLock().unlock();
        }
    }


    private void evictOldestEntries() {
        lock.writeLock().lock();
        try {
            if (skipMap.size() < maxEntries) {
                return;
            }
            Map.Entry<Long, Set<Long>> firstEntry = createTime2LedgerIds.pollFirstEntry();
            if (firstEntry != null) {
                Long createTime = firstEntry.getKey();
                Set<Long> ledgerIds = firstEntry.getValue();
                for (Long ledgerId : ledgerIds) {
                    removeLedger(ledgerId);
                }
                createTime2LedgerIds.remove(createTime);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeLedger(long ledgerId) {
        lock.writeLock().lock();
        try {
            CompositeKey startKey = new CompositeKey(ledgerId, 0);
            CompositeKey endKey = new CompositeKey(ledgerId, Long.MAX_VALUE);
            NavigableMap<CompositeKey, Long> ledgerEntries =
                    skipMap.subMap(startKey, true, endKey, true);
            ledgerEntries.clear();
            ledgerId2CreateTime.remove(ledgerId);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
