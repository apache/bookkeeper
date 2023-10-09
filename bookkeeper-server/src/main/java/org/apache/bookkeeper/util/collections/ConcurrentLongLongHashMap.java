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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.StampedLock;
import java.util.function.LongPredicate;

/**
 * Concurrent hash map from primitive long to long.
 *
 * <p>Provides similar methods as a {@code ConcurrentMap<K,V>} but since it's an open hash map with linear probing,
 * no node allocations are required to store the values.
 *
 * <p>Keys <strong>MUST</strong> be >= 0.
 *
 * <br>
 * <b>WARN: method forEach do not guarantee thread safety, nor do the keys, values and asMap method.</b>
 * <br>
 * The forEach method is specifically designed for single-threaded usage. When iterating over a map
 * with concurrent writes, it becomes possible for new values to be either observed or not observed.
 * There is no guarantee that if we write value1 and value2, and are able to see value2, then we will also see value1.
 * In some cases, it is even possible to encounter two mappings with the same key,
 * leading the keys method to return a List containing two identical keys.
 *
 * <br>
 * It is crucial to understand that the results obtained from aggregate status methods such as keys, values, and asMap
 * are typically reliable only when the map is not undergoing concurrent updates from other threads.
 * When concurrent updates are involved, the results of these methods reflect transient states
 * that may be suitable for monitoring or estimation purposes, but not for program control.
 */
public class ConcurrentLongLongHashMap {

    private static final long EmptyKey = -1L;
    private static final long DeletedKey = -2L;

    private static final long ValueNotFound = -1L;

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
     * Builder of ConcurrentLongLongHashMap.
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

        public ConcurrentLongLongHashMap build() {
            return new ConcurrentLongLongHashMap(expectedItems, concurrencyLevel,
                    mapFillFactor, mapIdleFactor, autoShrink, expandFactor, shrinkFactor);
        }
    }

    /**
     * A Long-Long BiConsumer.
     */
    public interface BiConsumerLong {
        void accept(long key, long value);
    }

    /**
     * A Long-Long function.
     */
    public interface LongLongFunction {
        long apply(long key);
    }

    /**
     * A Long-Long predicate.
     */
    public interface LongLongPredicate {
        boolean test(long key, long value);
    }

    @Deprecated
    public ConcurrentLongLongHashMap() {
        this(DefaultExpectedItems);
    }

    @Deprecated
    public ConcurrentLongLongHashMap(int expectedItems) {
        this(expectedItems, DefaultConcurrencyLevel);
    }

    @Deprecated
    public ConcurrentLongLongHashMap(int expectedItems, int concurrencyLevel) {
        this(expectedItems, concurrencyLevel, DefaultMapFillFactor, DefaultMapIdleFactor,
                DefaultAutoShrink, DefaultExpandFactor, DefaultShrinkFactor);
    }

    public ConcurrentLongLongHashMap(int expectedItems, int concurrencyLevel,
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
        int perSectionCapacity = (int) (perSectionExpectedItems / mapFillFactor);
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

    /**
     *
     * @param key
     * @return the value or -1 if the key was not present
     */
    public long get(long key) {
        checkBiggerEqualZero(key);
        long h = hash(key);
        return getSection(h).get(key, (int) h);
    }

    public boolean containsKey(long key) {
        return get(key) != ValueNotFound;
    }

    public long put(long key, long value) {
        checkBiggerEqualZero(key);
        checkBiggerEqualZero(value);
        long h = hash(key);
        return getSection(h).put(key, value, (int) h, false, null);
    }

    public long putIfAbsent(long key, long value) {
        checkBiggerEqualZero(key);
        checkBiggerEqualZero(value);
        long h = hash(key);
        return getSection(h).put(key, value, (int) h, true, null);
    }

    public long computeIfAbsent(long key, LongLongFunction provider) {
        checkBiggerEqualZero(key);
        checkNotNull(provider);
        long h = hash(key);
        return getSection(h).put(key, ValueNotFound, (int) h, true, provider);
    }

    /**
     * Atomically add the specified delta to a current value identified by the key. If the entry was not in the map, a
     * new entry with default value 0 is added and then the delta is added.
     *
     * @param key
     *            the entry key
     * @param delta
     *            the delta to add
     * @return the new value of the entry
     * @throws IllegalArgumentException
     *             if the delta was invalid, such as it would have caused the value to be < 0
     */
    public long addAndGet(long key, long delta) {
        checkBiggerEqualZero(key);
        long h = hash(key);
        return getSection(h).addAndGet(key, delta, (int) h);
    }

    /**
     * Change the value for a specific key only if it matches the current value.
     *
     * @param key
     * @param currentValue
     * @param newValue
     * @return
     */
    public boolean compareAndSet(long key, long currentValue, long newValue) {
        checkBiggerEqualZero(key);
        checkBiggerEqualZero(newValue);
        long h = hash(key);
        return getSection(h).compareAndSet(key, currentValue, newValue, (int) h);
    }

    /**
     * Remove an existing entry if found.
     *
     * @param key
     * @return the value associated with the key or -1 if key was not present
     */
    public long remove(long key) {
        checkBiggerEqualZero(key);
        long h = hash(key);
        return getSection(h).remove(key, ValueNotFound, (int) h);
    }

    public boolean remove(long key, long value) {
        checkBiggerEqualZero(key);
        checkBiggerEqualZero(value);
        long h = hash(key);
        return getSection(h).remove(key, value, (int) h) != ValueNotFound;
    }

    public int removeIf(LongPredicate filter) {
        checkNotNull(filter);

        int removedCount = 0;
        for (Section s : sections) {
            removedCount += s.removeIf(filter);
        }

        return removedCount;
    }

    public int removeIf(LongLongPredicate filter) {
        checkNotNull(filter);

        int removedCount = 0;
        for (Section s : sections) {
            removedCount += s.removeIf(filter);
        }

        return removedCount;
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

    public void forEach(BiConsumerLong processor) {
        for (Section s : sections) {
            s.forEach(processor);
        }
    }

    /**
     * @return a new list of all keys (makes a copy)
     */
    public List<Long> keys() {
        List<Long> keys = Lists.newArrayList();
        forEach((key, value) -> keys.add(key));
        return keys;
    }

    public List<Long> values() {
        List<Long> values = Lists.newArrayList();
        forEach((key, value) -> values.add(value));
        return values;
    }

    public Map<Long, Long> asMap() {
        Map<Long, Long> map = Maps.newHashMap();
        forEach((key, value) -> map.put(key, value));
        return map;
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
            this.table = new long[2 * this.capacity];
            this.size = 0;
            this.usedBuckets = 0;
            this.autoShrink = autoShrink;
            this.mapFillFactor = mapFillFactor;
            this.mapIdleFactor = mapIdleFactor;
            this.expandFactor = expandFactor;
            this.shrinkFactor = shrinkFactor;
            this.resizeThresholdUp = (int) (this.capacity * mapFillFactor);
            this.resizeThresholdBelow = (int) (this.capacity * mapIdleFactor);
            Arrays.fill(table, EmptyKey);
        }

        long get(long key, int keyHash) {
            long stamp = tryOptimisticRead();
            boolean acquiredLock = false;
            int bucket = signSafeMod(keyHash, capacity);

            try {
                while (true) {
                    // First try optimistic locking
                    long storedKey = table[bucket];
                    long storedValue = table[bucket + 1];

                    if (!acquiredLock && validate(stamp)) {
                        // The values we have read are consistent
                        if (key == storedKey) {
                            return storedValue;
                        } else if (storedKey == EmptyKey) {
                            // Not found
                            return ValueNotFound;
                        }
                    } else {
                        // Fallback to acquiring read lock
                        if (!acquiredLock) {
                            stamp = readLock();
                            acquiredLock = true;

                            bucket = signSafeMod(keyHash, capacity);
                            storedKey = table[bucket];
                            storedValue = table[bucket + 1];
                        }

                        if (key == storedKey) {
                            return storedValue;
                        } else if (storedKey == EmptyKey) {
                            // Not found
                            return ValueNotFound;
                        }
                    }

                    bucket = (bucket + 2) & (table.length - 1);
                }
            } finally {
                if (acquiredLock) {
                    unlockRead(stamp);
                }
            }
        }

        long put(long key, long value, int keyHash, boolean onlyIfAbsent, LongLongFunction valueProvider) {
            long stamp = writeLock();
            int bucket = signSafeMod(keyHash, capacity);

            // Remember where we find the first available spot
            int firstDeletedKey = -1;

            try {
                while (true) {
                    long storedKey = table[bucket];
                    long storedValue = table[bucket + 1];

                    if (key == storedKey) {
                        if (!onlyIfAbsent) {
                            // Over written an old value for same key
                            table[bucket + 1] = value;
                            return storedValue;
                        } else {
                            return storedValue;
                        }
                    } else if (storedKey == EmptyKey) {
                        // Found an empty bucket. This means the key is not in the map. If we've already seen a deleted
                        // key, we should write at that position
                        if (firstDeletedKey != -1) {
                            bucket = firstDeletedKey;
                        } else {
                            ++usedBuckets;
                        }

                        if (value == ValueNotFound) {
                            value = valueProvider.apply(key);
                        }

                        table[bucket] = key;
                        table[bucket + 1] = value;
                        ++size;
                        return valueProvider != null ? value : ValueNotFound;
                    } else if (storedKey == DeletedKey) {
                        // The bucket contained a different deleted key
                        if (firstDeletedKey == -1) {
                            firstDeletedKey = bucket;
                        }
                    }

                    bucket = (bucket + 2) & (table.length - 1);
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

        long addAndGet(long key, long delta, int keyHash) {
            long stamp = writeLock();
            int bucket = signSafeMod(keyHash, capacity);

            // Remember where we find the first available spot
            int firstDeletedKey = -1;

            try {
                while (true) {
                    long storedKey = table[bucket];
                    long storedValue = table[bucket + 1];

                    if (key == storedKey) {
                        // Over written an old value for same key
                        long newValue = storedValue + delta;
                        checkBiggerEqualZero(newValue);

                        table[bucket + 1] = newValue;
                        return newValue;
                    } else if (storedKey == EmptyKey) {
                        // Found an empty bucket. This means the key is not in the map. If we've already seen a deleted
                        // key, we should write at that position
                        checkBiggerEqualZero(delta);

                        if (firstDeletedKey != -1) {
                            bucket = firstDeletedKey;
                        } else {
                            ++usedBuckets;
                        }

                        table[bucket] = key;
                        table[bucket + 1] = delta;
                        ++size;
                        return delta;
                    } else if (storedKey == DeletedKey) {
                        // The bucket contained a different deleted key
                        if (firstDeletedKey == -1) {
                            firstDeletedKey = bucket;
                        }
                    }

                    bucket = (bucket + 2) & (table.length - 1);
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

        boolean compareAndSet(long key, long currentValue, long newValue, int keyHash) {
            long stamp = writeLock();
            int bucket = signSafeMod(keyHash, capacity);

            // Remember where we find the first available spot
            int firstDeletedKey = -1;

            try {
                while (true) {
                    long storedKey = table[bucket];
                    long storedValue = table[bucket + 1];

                    if (key == storedKey) {
                        if (storedValue != currentValue) {
                            return false;
                        }

                        // Over write an old value for same key
                        table[bucket + 1] = newValue;
                        return true;
                    } else if (storedKey == EmptyKey) {
                        // Found an empty bucket. This means the key is not in the map.
                        if (currentValue == -1) {
                            if (firstDeletedKey != -1) {
                                bucket = firstDeletedKey;
                            } else {
                                ++usedBuckets;
                            }

                            table[bucket] = key;
                            table[bucket + 1] = newValue;
                            ++size;
                            return true;
                        } else {
                            return false;
                        }
                    } else if (storedKey == DeletedKey) {
                        // The bucket contained a different deleted key
                        if (firstDeletedKey == -1) {
                            firstDeletedKey = bucket;
                        }
                    }

                    bucket = (bucket + 2) & (table.length - 1);
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

        private long remove(long key, long value, int keyHash) {
            long stamp = writeLock();
            int bucket = signSafeMod(keyHash, capacity);

            try {
                while (true) {
                    long storedKey = table[bucket];
                    long storedValue = table[bucket + 1];
                    if (key == storedKey) {
                        if (value == ValueNotFound || value == storedValue) {
                            --size;

                            cleanBucket(bucket);
                            return storedValue;
                        } else {
                            return ValueNotFound;
                        }
                    } else if (storedKey == EmptyKey) {
                        // Key wasn't found
                        return ValueNotFound;
                    }

                    bucket = (bucket + 2) & (table.length - 1);
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

        int removeIf(LongPredicate filter) {
            long stamp = writeLock();

            int removedCount = 0;
            try {
                // Go through all the buckets for this section
                for (int bucket = 0; size > 0 && bucket < table.length; bucket += 2) {
                    long storedKey = table[bucket];

                    if (storedKey != DeletedKey && storedKey != EmptyKey) {
                        if (filter.test(storedKey)) {
                            // Removing item
                            --size;
                            ++removedCount;
                            cleanBucket(bucket);
                        }
                    }
                }

                return removedCount;
            } finally {
                if (autoShrink && size < resizeThresholdBelow) {
                    try {
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

        int removeIf(LongLongPredicate filter) {
            long stamp = writeLock();

            int removedCount = 0;
            try {
                // Go through all the buckets for this section
                for (int bucket = 0; size > 0 && bucket < table.length; bucket += 2) {
                    long storedKey = table[bucket];
                    long storedValue = table[bucket + 1];

                    if (storedKey != DeletedKey && storedKey != EmptyKey) {
                        if (filter.test(storedKey, storedValue)) {
                            // Removing item
                            --size;
                            ++removedCount;
                            cleanBucket(bucket);
                        }
                    }
                }

                return removedCount;
            } finally {
                if (autoShrink && size < resizeThresholdBelow) {
                    try {
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
            int nextInArray = (bucket + 2) & (table.length - 1);
            if (table[nextInArray] == EmptyKey) {
                table[bucket] = EmptyKey;
                table[bucket + 1] = ValueNotFound;
                --usedBuckets;

                // Cleanup all the buckets that were in `DeletedKey` state, so that we can reduce unnecessary expansions
                bucket = (bucket - 2) & (table.length - 1);
                while (table[bucket] == DeletedKey) {
                    table[bucket] = EmptyKey;
                    table[bucket + 1] = ValueNotFound;
                    --usedBuckets;

                    bucket = (bucket - 2) & (table.length - 1);
                }
            } else {
                table[bucket] = DeletedKey;
                table[bucket + 1] = ValueNotFound;
            }
        }

        void clear() {
            long stamp = writeLock();

            try {
                if (autoShrink && capacity > initCapacity) {
                    shrinkToInitCapacity();
                } else {
                    Arrays.fill(table, EmptyKey);
                    this.size = 0;
                    this.usedBuckets = 0;
                }
            } finally {
                unlockWrite(stamp);
            }
        }

        public void forEach(BiConsumerLong processor) {
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
                for (int bucket = 0; bucket < table.length; bucket += 2) {
                    long storedKey = table[bucket];
                    long storedValue = table[bucket + 1];

                    if (!acquiredReadLock && !validate(stamp)) {
                        // Fallback to acquiring read lock
                        stamp = readLock();
                        acquiredReadLock = true;

                        storedKey = table[bucket];
                        storedValue = table[bucket + 1];
                    }

                    if (storedKey != DeletedKey && storedKey != EmptyKey) {
                        processor.accept(storedKey, storedValue);
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
            long[] newTable = new long[2 * newCapacity];
            Arrays.fill(newTable, EmptyKey);

            // Re-hash table
            for (int i = 0; i < table.length; i += 2) {
                long storedKey = table[i];
                long storedValue = table[i + 1];
                if (storedKey != EmptyKey && storedKey != DeletedKey) {
                    insertKeyValueNoLock(newTable, newCapacity, storedKey, storedValue);
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
            long[] newTable = new long[2 * initCapacity];
            Arrays.fill(newTable, EmptyKey);

            table = newTable;
            size = 0;
            usedBuckets = 0;
            // Capacity needs to be updated after the values, so that we won't see
            // a capacity value bigger than the actual array size
            capacity = initCapacity;
            resizeThresholdUp = (int) (capacity * mapFillFactor);
            resizeThresholdBelow = (int) (capacity * mapIdleFactor);
        }

        private static void insertKeyValueNoLock(long[] table, int capacity, long key, long value) {
            int bucket = signSafeMod(hash(key), capacity);

            while (true) {
                long storedKey = table[bucket];

                if (storedKey == EmptyKey) {
                    // The bucket is empty, so we can use it
                    table[bucket] = key;
                    table[bucket + 1] = value;
                    return;
                }

                bucket = (bucket + 2) & (table.length - 1);
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
        return (int) (n & (max - 1)) << 1;
    }

    private static int alignToPowerOfTwo(int n) {
        return (int) Math.pow(2, 32 - Integer.numberOfLeadingZeros(n - 1));
    }

    private static void checkBiggerEqualZero(long n) {
        if (n < 0L) {
            throw new IllegalArgumentException("Keys and values must be >= 0");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ConcurrentLongLongHashMap{");

        int headerLen = sb.length();
        forEach((k, v) -> {
                sb.append(k).append(" => ").append(v).append(", ");
            });
        if (sb.length() > headerLen) {
            sb.setLength(sb.length() - 2);
        }
        sb.append("}");
        return sb.toString();
    }
}
