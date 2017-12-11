/**
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
package org.apache.bookkeeper.util;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A snapshotable map.
 */
public class SnapshotMap<K, V> {
    // stores recent updates
    volatile Map<K, V> updates;
    volatile Map<K, V> updatesToMerge;
    // map stores all snapshot data
    volatile NavigableMap<K, V> snapshot;

    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public SnapshotMap() {
        updates = new ConcurrentHashMap<K, V>();
        updatesToMerge = new ConcurrentHashMap<K, V>();
        snapshot = new ConcurrentSkipListMap<K, V>();
    }

    /**
     * Create a snapshot of current map.
     *
     * @return a snapshot of current map.
     */
    public NavigableMap<K, V> snapshot() {
        this.lock.writeLock().lock();
        try {
            if (updates.isEmpty()) {
                return snapshot;
            }
            // put updates for merge to snapshot
            updatesToMerge = updates;
            updates = new ConcurrentHashMap<K, V>();
        } finally {
            this.lock.writeLock().unlock();
        }
        // merging the updates to snapshot
        for (Map.Entry<K, V> entry : updatesToMerge.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue());
        }
        // clear updatesToMerge
        this.lock.writeLock().lock();
        try {
            updatesToMerge = new ConcurrentHashMap<K, V>();
        } finally {
            this.lock.writeLock().unlock();
        }
        return snapshot;
    }

    /**
     * Associates the specified value with the specified key in this map.
     *
     * @param key
     *          Key with which the specified value is to be associated.
     * @param value
     *          Value to be associated with the specified key.
     */
    public void put(K key, V value) {
        this.lock.readLock().lock();
        try {
            updates.put(key, value);
        } finally {
            this.lock.readLock().unlock();
        }

    }

    /**
     * Removes the mapping for the key from this map if it is present.
     *
     * @param key
     *          Key whose mapping is to be removed from this map.
     */
    public void remove(K key) {
        this.lock.readLock().lock();
        try {
            // first remove updates
            updates.remove(key);
            updatesToMerge.remove(key);
            // then remove snapshot
            snapshot.remove(key);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * Returns true if this map contains a mapping for the specified key.
     *
     * @param key
     *          Key whose presence is in the map to be tested.
     * @return true if the map contains a mapping for the specified key.
     */
    public boolean containsKey(K key) {
        this.lock.readLock().lock();
        try {
            return updates.containsKey(key)
                 | updatesToMerge.containsKey(key)
                 | snapshot.containsKey(key);
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
