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
package org.apache.bookkeeper.common.allocator;

/**
 * Represents the action to take when it's not possible to allocate memory.
 */
public enum OutOfMemoryPolicy {

    /**
     * Throw regular OOM exception without taking addition actions.
     */
    ThrowException,

    /**
     * If it's not possible to allocate a buffer from direct memory, fallback to
     * allocate an unpooled buffer from JVM heap.
     *
     * <p>This will help absorb memory allocation spikes because the heap
     * allocations will naturally slow down the process and will result if full
     * GC cleanup if the Heap itself is full.
     */
    FallbackToHeap,
}
