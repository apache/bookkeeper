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
package org.apache.bookkeeper.util.collections;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;

/**
 * Concurrent hash set for primitive longs.
 *
 * <p>Provides similar methods as a ConcurrentSet&lt;Long&gt; but since it's an open hash map with linear probing,
 * no node allocations are required to store the values.
 *
 * <p>Items <strong>MUST</strong> be &gt;= 0.
 *
 * <br>
 * <b>WARN: method forEach do not guarantee thread safety, nor does the items method.</b>
 * <br>
 * The forEach method is specifically designed for single-threaded usage. When iterating over a set
 * with concurrent writes, it becomes possible for new values to be either observed or not observed.
 * There is no guarantee that if we write value1 and value2, and are able to see value2, then we will also see value1.
 *
 * <br>
 * It is crucial to understand that the results obtained from aggregate status methods such as items
 * are typically reliable only when the map is not undergoing concurrent updates from other threads.
 * When concurrent updates are involved, the results of these methods reflect transient states
 * that may be suitable for monitoring or estimation purposes, but not for program control.
 */
public class ConcurrentLongHashSet {

    private static final long EmptyItem = -1L;
    private static final long DeletedItem = -2L;

    private static final float SetFillFactor = 0.66f;

    private static final int DefaultExpectedItems = 256;
    private static final int DefaultConcurrencyLevel = 16;

    private static final float DefaultMapFillFactor = 0.66f;
    private static final float DefaultMapIdleFactor = 0.15f;

    private static final float DefaultExpandFactor = 2;
    private static final float DefaultShrinkFactor = 2;

    private static final boolean DefaultAutoShrink = false;

    private final Section[] sections;

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder of ConcurrentLongHashSet.
     */
    public static class Builder {
        int expectedItems = DefaultExpectedItems;
        int concurrencyLevel = DefaultConcurrencyLevel;
        float mapFillFactor = DefaultMapFillFactor;
        float mapIdleFactor = DefaultMapIdleFactor;
        float expandFactor = DefaultExpandFactor;
        float shrinkFactor = DefaultShrinkFactor;
        boolean autoShrink = DefaultAutoShrink;

        public Builder expectedItems(int expectedItems) {
            this.expectedItems = expectedItems;
            return this;
        }

        public Builder concurrencyLevel(int concurrencyLevel) {
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }

        public Builder mapFillFactor(float mapFillFactor) {
            this.mapFillFactor = mapFillFactor;
            return this;
        }

        public Builder mapIdleFactor(float mapIdleFactor) {
            this.mapIdleFactor = mapIdleFactor;
            return this;
        }

        public Builder expandFactor(float expandFactor) {
            this.expandFactor = expandFactor;
            return this;
        }

        public Builder shrinkFactor(float shrinkFactor) {
            this.shrinkFactor = shrinkFactor;
            return this;
        }

        public Builder autoShrink(boolean autoShrink) {
            this.autoShrink = autoShrink;
            return this;
        }

        public ConcurrentLongHashSet build() {
            return new ConcurrentLongHashSet(expectedItems, concurrencyLevel,
                    mapFillFactor, mapIdleFactor, autoShrink, expandFactor, shrinkFactor);
        }
    }


    /**
     * A consumer of long values.
     */
    public interface ConsumerLong {
        void accept(long item);
    }

    @Deprecated
    public ConcurrentLongHashSet() {
        this(DefaultExpectedItems);
    }

    @Deprecated
    public ConcurrentLongHashSet(int expectedItems) {
        this(expectedItems, DefaultConcurrencyLevel);
    }

    @Deprecated
    public ConcurrentLongHashSet(int expectedItems, int concurrencyLevel) {
        this(expectedItems, concurrencyLevel, DefaultMapFillFactor, DefaultMapIdleFactor,
                DefaultAutoShrink, DefaultExpandFactor, DefaultShrinkFactor);
    }

    public ConcurrentLongHashSet(int expectedItems, int concurrencyLevel,
                                 float mapFillFactor, float mapIdleFactor,
                                 boolean autoShrink, float expandFactor, float shrinkFactor) {
        checkArgument(expectedItems > 0);
        checkArgument(concurrencyLevel > 0);
        checkArgument(expectedItems >= concurrencyLevel);
        checkArgument(mapFillFactor > 0 && mapFillFactor < 1);
        checkArgument(mapIdleFactor > 0 && mapIdleFactor < 1);
        checkArgument(mapFillFactor > mapIdleFactor);
        checkArgument(expandFactor > 1);
        checkArgument(shrinkFactor > 1);

        int numSections = concurrencyLevel;
        int perSectionExpectedItems = expectedItems / numSections;
        int perSectionCapacity = (int) (perSectionExpectedItems / SetFillFactor);
        this.sections = new Section[numSections];

        for (int i = 0; i < numSections; i++) {
            sections[i] = new Section(perSectionCapacity, mapFillFactor, mapIdleFactor,
                    autoShrink, expandFactor, shrinkFactor);
        }
    }

    public long size() {
        long size = 0;
        for (Section s : sections) {
            size += s.size;
        }
        return size;
    }

    public long sizeInBytes() {
        long size = 0;
        for (Section s : sections) {
            size += (long) s.table.length * Long.BYTES;
        }
        return size;
    }

    public long capacity() {
        long capacity = 0;
        for (Section s : sections) {
            capacity += s.capacity;
        }
        return capacity;
    }

    public boolean isEmpty() {
        for (Section s : sections) {
            if (s.size != 0) {
                return false;
            }
        }

        return true;
    }

    long getUsedBucketCount() {
        long usedBucketCount = 0;
        for (Section s : sections) {
            usedBucketCount += s.usedBuckets;
        }
        return usedBucketCount;
    }

    public boolean contains(long item) {
        checkBiggerEqualZero(item);
        long h = hash(item);
        return getSection(h).contains(item, (int) h);
    }

    public boolean add(long item) {
        checkBiggerEqualZero(item);
        long h = hash(item);
        return getSection(h).add(item, (int) h);
    }

    /**
     * Remove an existing entry if found.
     *
     * @param item
     * @return true if removed or false if item was not present
     */
    public boolean remove(long item) {
        checkBiggerEqualZero(item);
        long h = hash(item);
        return getSection(h).remove(item, (int) h);
    }

    private Section getSection(long hash) {
        // Use 32 msb out of long to get the section
        final int sectionIdx = (int) (hash >>> 32) & (sections.length - 1);
        return sections[sectionIdx];
    }

    public void clear() {
        for (Section s : sections) {
            s.clear();
        }
    }

    public void forEach(ConsumerLong processor) {
        for (Section s : sections) {
            s.forEach(processor);
        }
    }

    /**
     * @return a new list of all keys (makes a copy)
     */
    public Set<Long> items() {
        Set<Long> items = new HashSet<>();
        forEach(items::add);
        return items;
    }

    // A section is a portion of the hash map that is covered by a single
    @SuppressWarnings("serial")
    private static final class Section extends StampedLock {
        // Keys and values are stored interleaved in the table array
        private volatile long[] table;

        private volatile int capacity;
        private final int initCapacity;
        private volatile int size;
        private int usedBuckets;
        private int resizeThresholdUp;
        private int resizeThresholdBelow;
        private final float mapFillFactor;
        private final float mapIdleFactor;
        private final float expandFactor;
        private final float shrinkFactor;
        private final boolean autoShrink;

        Section(int capacity, float mapFillFactor, float mapIdleFactor, boolean autoShrink,
                float expandFactor, float shrinkFactor) {
            this.capacity = alignToPowerOfTwo(capacity);
            this.initCapacity = this.capacity;
            this.table = new long[this.capacity];
            this.size = 0;
            this.usedBuckets = 0;
            this.autoShrink = autoShrink;
            this.mapFillFactor = mapFillFactor;
            this.mapIdleFactor = mapIdleFactor;
            this.expandFactor = expandFactor;
            this.shrinkFactor = shrinkFactor;
            this.resizeThresholdUp = (int) (this.capacity * mapFillFactor);
            this.resizeThresholdBelow = (int) (this.capacity * mapIdleFactor);
            Arrays.fill(table, EmptyItem);
        }

        boolean contains(long item, int hash) {
            long stamp = tryOptimisticRead();
            boolean acquiredLock = false;
            int bucket = signSafeMod(hash, capacity);

            try {
                while (true) {
                    // First try optimistic locking
                    long storedItem = table[bucket];

                    if (!acquiredLock && validate(stamp)) {
                        // The values we have read are consistent
                        if (item == storedItem) {
                            return true;
                        } else if (storedItem == EmptyItem) {
                            // Not found
                            return false;
                        }
                    } else {
                        // Fallback to acquiring read lock
                        if (!acquiredLock) {
                            stamp = readLock();
                            acquiredLock = true;

                            bucket = signSafeMod(hash, capacity);
                            storedItem = table[bucket];
                        }

                        if (item == storedItem) {
                            return true;
                        } else if (storedItem == EmptyItem) {
                            // Not found
                            return false;
                        }
                    }

                    bucket = (bucket + 1) & (table.length - 1);
                }
            } finally {
                if (acquiredLock) {
                    unlockRead(stamp);
                }
            }
        }

        boolean add(long item, long hash) {
            long stamp = writeLock();
            int bucket = signSafeMod(hash, capacity);

            // Remember where we find the first available spot
            int firstDeletedItem = -1;

            try {
                while (true) {
                    long storedItem = table[bucket];

                    if (item == storedItem) {
                        // Item was already in set
                        return false;
                    } else if (storedItem == EmptyItem) {
                        // Found an empty bucket. This means the key is not in the map. If we've already seen a deleted
                        // key, we should write at that position
                        if (firstDeletedItem != -1) {
                            bucket = firstDeletedItem;
                        } else {
                            ++usedBuckets;
                        }

                        table[bucket] = item;
                        ++size;
                        return true;
                    } else if (storedItem == DeletedItem) {
                        // The bucket contained a different deleted key
                        if (firstDeletedItem == -1) {
                            firstDeletedItem = bucket;
                        }
                    }

                    bucket = (bucket + 1) & (table.length - 1);
                }
            } finally {
                if (usedBuckets > resizeThresholdUp) {
                    try {
                        // Expand the hashmap
                        int newCapacity = alignToPowerOfTwo((int) (capacity * expandFactor));
                        rehash(newCapacity);
                    } finally {
                        unlockWrite(stamp);
                    }
                } else {
                    unlockWrite(stamp);
                }
            }
        }

        private boolean remove(long item, int hash) {
            long stamp = writeLock();
            int bucket = signSafeMod(hash, capacity);

            try {
                while (true) {
                    long storedItem = table[bucket];
                    if (item == storedItem) {
                        --size;

                        cleanBucket(bucket);
                        return true;

                    } else if (storedItem == EmptyItem) {
                        // Key wasn't found
                        return false;
                    }

                    bucket = (bucket + 1) & (table.length - 1);
                }
            } finally {
                if (autoShrink && size < resizeThresholdBelow) {
                    try {
                        // Shrinking must at least ensure initCapacity,
                        // so as to avoid frequent shrinking and expansion near initCapacity,
                        // frequent shrinking and expansion,
                        // additionally opened arrays will consume more memory and affect GC
                        int newCapacity = Math.max(alignToPowerOfTwo((int) (capacity / shrinkFactor)), initCapacity);
                        int newResizeThresholdUp = (int) (newCapacity * mapFillFactor);
                        if (newCapacity < capacity && newResizeThresholdUp > size) {
                            // shrink the hashmap
                            rehash(newCapacity);
                        }
                    } finally {
                        unlockWrite(stamp);
                    }
                } else {
                    unlockWrite(stamp);
                }
            }
        }

        private void cleanBucket(int bucket) {
            int nextInArray = (bucket + 1) & (table.length - 1);
            if (table[nextInArray] == EmptyItem) {
                table[bucket] = EmptyItem;
                --usedBuckets;

                // Cleanup all the buckets that were in `DeletedKey` state,
                // so that we can reduce unnecessary expansions
                bucket = (bucket - 1) & (table.length - 1);
                while (table[bucket] == DeletedItem) {
                    table[bucket] = EmptyItem;
                    --usedBuckets;

                    bucket = (bucket - 1) & (table.length - 1);
                }
            } else {
                table[bucket] = DeletedItem;
            }
        }

        void clear() {
            long stamp = writeLock();

            try {
                if (autoShrink && capacity > initCapacity) {
                    shrinkToInitCapacity();
                } else {
                    Arrays.fill(table, EmptyItem);
                    this.size = 0;
                    this.usedBuckets = 0;
                }
            } finally {
                unlockWrite(stamp);
            }
        }

        public void forEach(ConsumerLong processor) {
            long stamp = tryOptimisticRead();

            long[] table = this.table;
            boolean acquiredReadLock = false;

            try {

                // Validate no rehashing
                if (!validate(stamp)) {
                    // Fallback to read lock
                    stamp = readLock();
                    acquiredReadLock = true;
                    table = this.table;
                }

                // Go through all the buckets for this section
                for (int bucket = 0; bucket < table.length; bucket++) {
                    long storedItem = table[bucket];

                    if (!acquiredReadLock && !validate(stamp)) {
                        // Fallback to acquiring read lock
                        stamp = readLock();
                        acquiredReadLock = true;

                        storedItem = table[bucket];
                    }

                    if (storedItem != DeletedItem && storedItem != EmptyItem) {
                        processor.accept(storedItem);
                    }
                }
            } finally {
                if (acquiredReadLock) {
                    unlockRead(stamp);
                }
            }
        }

        private void rehash(int newCapacity) {
            // Expand the hashmap
            long[] newTable = new long[newCapacity];
            Arrays.fill(newTable, EmptyItem);

            // Re-hash table
            for (int i = 0; i < table.length; i++) {
                long storedItem = table[i];
                if (storedItem != EmptyItem && storedItem != DeletedItem) {
                    insertKeyValueNoLock(newTable, newCapacity, storedItem);
                }
            }

            table = newTable;
            usedBuckets = size;
            // Capacity needs to be updated after the values, so that we won't see
            // a capacity value bigger than the actual array size
            capacity = newCapacity;
            resizeThresholdUp = (int) (capacity * mapFillFactor);
            resizeThresholdBelow = (int) (capacity * mapIdleFactor);
        }

        private void shrinkToInitCapacity() {
            long[] newTable = new long[initCapacity];
            Arrays.fill(newTable, EmptyItem);

            table = newTable;
            size = 0;
            usedBuckets = 0;
            // Capacity needs to be updated after the values, so that we won't see
            // a capacity value bigger than the actual array size
            capacity = initCapacity;
            resizeThresholdUp = (int) (capacity * mapFillFactor);
            resizeThresholdBelow = (int) (capacity * mapIdleFactor);
        }

        private static void insertKeyValueNoLock(long[] table, int capacity, long item) {
            int bucket = signSafeMod(hash(item), capacity);

            while (true) {
                long storedKey = table[bucket];

                if (storedKey == EmptyItem) {
                    // The bucket is empty, so we can use it
                    table[bucket] = item;
                    return;
                }

                bucket = (bucket + 1) & (table.length - 1);
            }
        }
    }

    private static final long HashMixer = 0xc6a4a7935bd1e995L;
    private static final int R = 47;

    static final long hash(long key) {
        long hash = key * HashMixer;
        hash ^= hash >>> R;
        hash *= HashMixer;
        return hash;
    }

    static final int signSafeMod(long n, int max) {
        return (int) (n & (max - 1));
    }

    private static int alignToPowerOfTwo(int n) {
        return (int) Math.pow(2, 32 - Integer.numberOfLeadingZeros(n - 1));
    }

    private static void checkBiggerEqualZero(long n) {
        if (n < 0L) {
            throw new IllegalArgumentException("Keys and values must be >= 0");
        }
    }
}
