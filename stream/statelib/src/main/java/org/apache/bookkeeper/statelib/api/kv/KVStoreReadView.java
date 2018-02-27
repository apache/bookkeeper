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
 * A read-only view of a key/value store. It only supports read-only operations.
 *
 * <p>The implementation of this class should be thread-safe.
 */
@Public
@Evolving
public interface KVStoreReadView<K, V> {

    /**
     * Return the value associated with this key.
     *
     * @param key key to retrieve the value
     * @return the value associated with this key.
     */
    V get(K key);

    /**
     * Return an {@link KVIterator} to iterate a range of keys.
     *
     * @param from the first key
     * @param to the last key
     * @return a k/v iterator
     */
    KVIterator<K, V> range(K from, K to);


}
