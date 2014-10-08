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

package org.apache.hedwig.jms;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
* An LRUCache (set) based on the LRUCacheMap.
*/
public class LRUCacheSet<E> implements Set<E> {

    private final int maxSize;
    private final Map<E, E> cache;

    public LRUCacheSet(int maxSize, boolean accessOrder){
        this.maxSize = maxSize;

        this.cache = new LinkedHashMap<E, E>(maxSize, 0.75f, accessOrder){
            @Override
            protected boolean removeEldestEntry(Map.Entry<E, E> eldest) {
                boolean retval = super.removeEldestEntry(eldest);
                return retval || super.size() > LRUCacheSet.this.maxSize;
            }
        };
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public boolean isEmpty() {
        return cache.isEmpty();
    }

    @Override
    public boolean contains(Object val) {
        return cache.containsKey(val);
    }

    @Override
    public Iterator<E> iterator() {
        return cache.keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return cache.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return cache.keySet().toArray(a);
    }

    @Override
    public boolean add(E val) {
        return null == cache.put(val, val);
    }

    @Override
    public boolean remove(Object val) {
        if (! contains(val)) return false;
        cache.remove(val);
        return true;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return cache.keySet().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean retval = false;
        for (E e : c) retval = retval | add(e);
        return retval;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return cache.keySet().retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return cache.keySet().removeAll(c);
    }

    @Override
    public void clear() {
        cache.clear();
    }
}
