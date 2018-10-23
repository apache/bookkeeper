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
 * Define a policy for allocating buffers.
 */
public enum PoolingPolicy {

    /**
     * Allocate memory from JVM heap without any pooling.
     *
     * <p>This option has the least overhead in terms of memory usage since the
     * memory will be automatically reclaimed by the JVM GC but might impose a
     * performance penalty at high throughput.
     */
    UnpooledHeap,

    /**
     * Use Direct memory for all buffers and pool the memory.
     *
     * <p>Direct memory will avoid the overhead of JVM GC and most memory copies
     * when reading and writing to socket channel.
     *
     * <p>Pooling will add memory space overhead due to the fact that there will be
     * fragmentation in the allocator and that threads will keep a portion of
     * memory as thread-local to avoid contention when possible.
     */
    PooledDirect
}
