/*
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
 */
package org.apache.bookkeeper.common.collections;

import java.io.Serializable;
import lombok.EqualsAndHashCode;

/**
 * An immutable pair of objects.
 */
@EqualsAndHashCode
public final class Pair<K, V> implements Serializable {

    private static final long serialVersionUID = 2343L;

    private final K left;
    private final V right;

    /**
     * Creates an immutable pair.
     *
     * @param left the left object
     * @param right the right object
     * @return a new Pair of objects
     */
    public static <K, V> Pair<K, V> of(K left, V right) {
        return new Pair<>(left, right);
    }

    private Pair(K left, V right) {
        this.left = left;
        this.right = right;
    }

    /**
     * Gets the left object.
     *
     * @return the left object
     */
    public K getLeft() {
        return left;
    }

    /**
     * Gets the right object.
     *
     * @return the right object
     */
    public V getRight() {
        return right;
    }

}
