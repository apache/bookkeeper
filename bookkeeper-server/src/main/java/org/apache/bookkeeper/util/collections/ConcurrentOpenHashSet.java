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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

/**
 * Concurrent hash set.
 *
 * <p>Provides similar methods as a {@code ConcurrentMap<K,V>} but since it's an open hash map with linear probing,
 * no node allocations are required to store the values
 *
 * @param <V>
 */
@SuppressWarnings("unchecked")
public class ConcurrentOpenHashSet<V> {

    private static final Object EmptyValue = null;
    private static final Object DeletedValue = new Object();

    private static final int DefaultExpectedItems = 256;
    private static final int DefaultConcurrencyLevel = 16;

    private static final float DefaultMapFillFactor = 0.66f;
    private static final float DefaultMapIdleFactor = 0.15f;

    private static final float DefaultExpandFactor = 2;
    private static final float DefaultShrinkFactor = 2;

    private static final boolean DefaultAutoShrink = false;

    private final Section<V>[] sections;

    public static <V> Builder<V> newBuilder() {
        return new Builder<>();
    }

    /**
     * Builder of ConcurrentOpenHashSet.
     */
    public static class Builder<V> {
        int expectedItems = DefaultExpectedItems;
        int concurrencyLevel = DefaultConcurrencyLevel;
        float mapFillFactor = DefaultMapFillFactor;
        float mapIdleFactor = DefaultMapIdleFactor;
        float expandFactor = DefaultExpandFactor;
        float shrinkFactor = DefaultShrinkFactor;
        boolean autoShrink = DefaultAutoShrink;

        public Builder<V> expectedItems(int expectedItems) {
            this.expectedItems = expectedItems;
            return this;
        }

        public Builder<V> concurrencyLevel(int concurrencyLevel) {
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }

        public Builder<V> mapFillFactor(float mapFillFactor) {
            this.mapFillFactor = mapFillFactor;
            return this;
        }

        public Builder<V> mapIdleFactor(float mapIdleFactor) {
            this.mapIdleFactor = mapIdleFactor;
            return this;
        }

        public Builder<V> expandFactor(float expandFactor) {
            this.expandFactor = expandFactor;
            return this;
        }

        public Builder<V> shrinkFactor(float shrinkFactor) {
            this.shrinkFactor = shrinkFactor;
            return this;
        }

        public Builder<V> autoShrink(boolean autoShrink) {
            this.autoShrink = autoShrink;
            return this;
        }

        public ConcurrentOpenHashSet<V> build() {
            return new ConcurrentOpenHashSet<>(expectedItems, concurrencyLevel,
                    mapFillFactor, mapIdleFactor, autoShrink, expandFactor, shrinkFactor);
        }
    }

    @Deprecated
    public ConcurrentOpenHashSet() {
        this(DefaultExpectedItems);
    }

    @Deprecated
    public ConcurrentOpenHashSet(int expectedItems) {
        this(expectedItems, DefaultConcurrencyLevel);
    }

    @Deprecated
    public ConcurrentOpenHashSet(int expectedItems, int concurrencyLevel) {
        this(expectedItems, concurrencyLevel, DefaultMapFillFactor, DefaultMapIdleFactor,
                DefaultAutoShrink, DefaultExpandFactor, DefaultShrinkFactor);
    }

    public ConcurrentOpenHashSet(int expectedItems, int concurrencyLevel,
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
            sections[i] = new Section<>(perSectionCapacity, mapFillFactor, mapIdleFactor,
                    autoShrink, expandFactor, shrinkFactor);
        }
    }

    public long size() {
        long size = 0;
        for (Section<V> s : sections) {
            size += s.size;
        }
        return size;
    }

    public long capacity() {
        long capacity = 0;
        for (Section<V> s : sections) {
            capacity += s.capacity;
        }
        return capacity;
    }

    long getUsedBucketCount() {
        long usedBucketCount = 0;
        for (Section<V> s : sections) {
            usedBucketCount += s.usedBuckets;
        }
        return usedBucketCount;
    }

    public boolean isEmpty() {
        for (Section<V> s : sections) {
            if (s.size != 0) {
                return false;
            }
        }

        return true;
    }

    public boolean contains(V value) {
        checkNotNull(value);
        long h = hash(value);
        return getSection(h).contains(value, (int) h);
    }

    public boolean add(V value) {
        checkNotNull(value);
        long h = hash(value);
        return getSection(h).add(value, (int) h);
    }

    public boolean remove(V value) {
        checkNotNull(value);
        long h = hash(value);
        return getSection(h).remove(value, (int) h);
    }

    private Section<V> getSection(long hash) {
        // Use 32 msb out of long to get the section
        final int sectionIdx = (int) (hash >>> 32) & (sections.length - 1);
        return sections[sectionIdx];
    }

    public void clear() {
        for (Section<V> s : sections) {
            s.clear();
        }
    }

    public void forEach(Consumer<? super V> processor) {
        for (Section<V> s : sections) {
            s.forEach(processor);
        }
    }

    /**
     * @return a new list of all values (makes a copy)
     */
    List<V> values() {
        List<V> values = Lists.newArrayList();
        forEach(value -> values.add(value));
        return values;
    }

    // A section is a portion of the hash map that is covered by a single
    @SuppressWarnings("serial")
    private static final class Section<V> extends StampedLock {
        private volatile V[] values;

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
            this.values = (V[]) new Object[this.capacity];
            this.size = 0;
            this.usedBuckets = 0;
            this.autoShrink = autoShrink;
            this.mapFillFactor = mapFillFactor;
            this.mapIdleFactor = mapIdleFactor;
            this.expandFactor = expandFactor;
            this.shrinkFactor = shrinkFactor;
            this.resizeThresholdUp = (int) (this.capacity * mapFillFactor);
            this.resizeThresholdBelow = (int) (this.capacity * mapIdleFactor);
        }

        boolean contains(V value, int keyHash) {
            int bucket = keyHash;

            long stamp = tryOptimisticRead();
            boolean acquiredLock = false;

            try {
                while (true) {
                    int capacity = this.capacity;
                    bucket = signSafeMod(bucket, capacity);

                    // First try optimistic locking
                    V storedValue = values[bucket];

                    if (!acquiredLock && validate(stamp)) {
                        // The values we have read are consistent
                        if (value.equals(storedValue)) {
                            return true;
                        } else if (storedValue == EmptyValue) {
                            // Not found
                            return false;
                        }
                    } else {
                        // Fallback to acquiring read lock
                        if (!acquiredLock) {
                            stamp = readLock();
                            acquiredLock = true;

                            storedValue = values[bucket];
                        }

                        if (capacity != this.capacity) {
                            // There has been a rehashing. We need to restart the search
                            bucket = keyHash;
                            continue;
                        }

                        if (value.equals(storedValue)) {
                            return true;
                        } else if (storedValue == EmptyValue) {
                            // Not found
                            return false;
                        }
                    }

                    ++bucket;
                }
            } finally {
                if (acquiredLock) {
                    unlockRead(stamp);
                }
            }
        }

        boolean add(V value, int keyHash) {
            int bucket = keyHash;

            long stamp = writeLock();
            int capacity = this.capacity;

            // Remember where we find the first available spot
            int firstDeletedValue = -1;

            try {
                while (true) {
                    bucket = signSafeMod(bucket, capacity);

                    V storedValue = values[bucket];

                    if (value.equals(storedValue)) {
                        return false;
                    } else if (storedValue == EmptyValue) {
                        // Found an empty bucket. This means the value is not in the set. If we've already seen a
                        // deleted value, we should write at that position
                        if (firstDeletedValue != -1) {
                            bucket = firstDeletedValue;
                        } else {
                            ++usedBuckets;
                        }

                        values[bucket] = value;
                        ++size;
                        return true;
                    } else if (storedValue == DeletedValue) {
                        // The bucket contained a different deleted key
                        if (firstDeletedValue == -1) {
                            firstDeletedValue = bucket;
                        }
                    }

                    ++bucket;
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

        private boolean remove(V value, int keyHash) {
            int bucket = keyHash;
            long stamp = writeLock();

            try {
                while (true) {
                    int capacity = this.capacity;
                    bucket = signSafeMod(bucket, capacity);

                    V storedValue = values[bucket];
                    if (value.equals(storedValue)) {
                        --size;

                        int nextInArray = signSafeMod(bucket + 1, capacity);
                        if (values[nextInArray] == EmptyValue) {
                            values[bucket] = (V) EmptyValue;
                            --usedBuckets;

                            // Cleanup all the buckets that were in `DeletedValue` state,
                            // so that we can reduce unnecessary expansions
                            int lastBucket = signSafeMod(bucket - 1, capacity);
                            while (values[lastBucket] == DeletedValue) {
                                values[lastBucket] = (V) EmptyValue;
                                --usedBuckets;

                                lastBucket = signSafeMod(--lastBucket, capacity);
                            }
                        } else {
                            values[bucket] = (V) DeletedValue;
                        }

                        return true;
                    } else if (storedValue == EmptyValue) {
                        // Value wasn't found
                        return false;
                    }

                    ++bucket;
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

        void clear() {
            long stamp = writeLock();

            try {
                if (autoShrink && capacity > initCapacity) {
                    shrinkToInitCapacity();
                } else {
                    Arrays.fill(values, EmptyValue);
                    this.size = 0;
                    this.usedBuckets = 0;
                }
            } finally {
                unlockWrite(stamp);
            }
        }

        public void forEach(Consumer<? super V> processor) {
            long stamp = tryOptimisticRead();

            int capacity = this.capacity;
            V[] values = this.values;

            boolean acquiredReadLock = false;

            try {

                // Validate no rehashing
                if (!validate(stamp)) {
                    // Fallback to read lock
                    stamp = readLock();
                    acquiredReadLock = true;

                    capacity = this.capacity;
                    values = this.values;
                }

                // Go through all the buckets for this section
                for (int bucket = 0; bucket < capacity; bucket++) {
                    V storedValue = values[bucket];

                    if (!acquiredReadLock && !validate(stamp)) {
                        // Fallback to acquiring read lock
                        stamp = readLock();
                        acquiredReadLock = true;

                        storedValue = values[bucket];
                    }

                    if (storedValue != DeletedValue && storedValue != EmptyValue) {
                        processor.accept(storedValue);
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
            V[] newValues = (V[]) new Object[newCapacity];

            // Re-hash table
            for (int i = 0; i < values.length; i++) {
                V storedValue = values[i];
                if (storedValue != EmptyValue && storedValue != DeletedValue) {
                    insertValueNoLock(newValues, storedValue);
                }
            }

            values = newValues;
            usedBuckets = size;
            // Capacity needs to be updated after the values, so that we won't see
            // a capacity value bigger than the actual array size
            capacity = newCapacity;
            resizeThresholdUp = (int) (capacity * mapFillFactor);
            resizeThresholdBelow = (int) (capacity * mapIdleFactor);
        }

        private void shrinkToInitCapacity() {
            V[] newValues = (V[]) new Object[initCapacity];

            values = newValues;
            size = 0;
            usedBuckets = 0;
            // Capacity needs to be updated after the values, so that we won't see
            // a capacity value bigger than the actual array size
            capacity = initCapacity;
            resizeThresholdUp = (int) (capacity * mapFillFactor);
            resizeThresholdBelow = (int) (capacity * mapIdleFactor);
        }

        private static <V> void insertValueNoLock(V[] values, V value) {
            int bucket = (int) hash(value);

            while (true) {
                bucket = signSafeMod(bucket, values.length);

                V storedValue = values[bucket];

                if (storedValue == EmptyValue) {
                    // The bucket is empty, so we can use it
                    values[bucket] = value;
                    return;
                }

                ++bucket;
            }
        }
    }

    private static final long HashMixer = 0xc6a4a7935bd1e995L;
    private static final int R = 47;

    static final <K> long hash(K key) {
        long hash = key.hashCode() * HashMixer;
        hash ^= hash >>> R;
        hash *= HashMixer;
        return hash;
    }

    static final int signSafeMod(long n, int max) {
        return (int) n & (max - 1);
    }

    private static int alignToPowerOfTwo(int n) {
        return (int) Math.pow(2, 32 - Integer.numberOfLeadingZeros(n - 1));
    }
}
