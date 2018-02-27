/*
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

package org.apache.bookkeeper.statelib.api.kv;

import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;

/**
 * The write view for a key/value store that supports write operations, such as put and delete.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@Public
@Evolving
public interface KVStoreWriteView<K, V> {

    /**
     * Update the <code>value</code> associated with the <code>key</code>.
     *
     * @param key   the key to update
     * @param value the new value to associate with the key
     * @throws NullPointerException if a null key is provided
     */
    void put(K key, V value);

    /**
     * Update the <code>value</code> associated with the <code>key</code>,
     * unless a value is already associated with the key.
     *
     * @param key   the key to update
     * @param value the new value to associate with the key
     * @return the old value if there is already a value associated with the key. the <i>put</i> operation
     * is treated as failure in this case. or null if there is no such key. the <i>put</i> operation succeeds
     * in this case.
     * @throws NullPointerException if a null key is provided
     */
    V putIfAbsent(K key, V value);

    /**
     * Returns a <i>multi</i> operation to build a multi-key update to modify the key/value state store.
     *
     * @return a multi operation to modify the key/value state store.
     */
    KVMulti<K, V> multi();

    /**
     * Delete the value associated with the key from the store.
     *
     * @param key the key to delete
     * @return the old value if there is already a value associated with the kye, or null if there is no such key.
     * @throws NullPointerException if a null key is provided
     */
    V delete(K key);

}
