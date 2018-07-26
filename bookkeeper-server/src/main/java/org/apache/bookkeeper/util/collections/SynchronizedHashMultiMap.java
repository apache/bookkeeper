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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Simple multimap implementation that only stores key reference once.
 *
 * <p>Implementation is aimed at storing PerChannelBookieClient completions when there
 * are duplicates. If the key is a pooled object, it must not exist once the value
 * has been removed from the map, which can happen with guava multimap implemenations.
 *
 * <p>With this map is implemented with pretty heavy locking, but this shouldn't be an
 * issue as the multimap only needs to be used in rare cases, i.e. when a user tries
 * to read or the same entry twice at the same time. This class should *NOT*  be used
 * in critical path code.
 *
 * <p>A unique key-value pair will only be stored once.
 */
public class SynchronizedHashMultiMap<K, V> {

    HashMap<Integer, Set<Pair<K, V>>> map = new HashMap<>();

    public synchronized void put(K k, V v) {
        map.computeIfAbsent(k.hashCode(), (ignore) -> new HashSet<>()).add(Pair.of(k, v));
    }

    public synchronized Optional<K> getAnyKey() {
        return map.values().stream().findAny().flatMap(pairs -> pairs.stream().findAny().map(p -> p.getLeft()));
    }

    public synchronized Optional<V> removeAny(K k) {
        Set<Pair<K, V>> set = map.getOrDefault(k.hashCode(), Collections.emptySet());
        Optional<Pair<K, V>> pair = set.stream().filter(p -> p.getLeft().equals(k)).findAny();
        pair.ifPresent(p -> set.remove(p));
        return pair.map(p -> p.getRight());
    }

    public synchronized int removeIf(BiPredicate<K, V> predicate) {
        int removedSum = map.values().stream().mapToInt(
                pairs -> {
                    int removed = 0;
                    // Can't use removeIf because we need the count
                    Iterator<Pair<K, V>> iter = pairs.iterator();
                    while (iter.hasNext()) {
                        Pair<K, V> kv = iter.next();
                        if (predicate.test(kv.getLeft(), kv.getRight())) {
                            iter.remove();
                            removed++;
                        }
                    }
                    return removed;
                }).sum();
        map.values().removeIf((s) -> s.isEmpty());
        return removedSum;
    }
}
