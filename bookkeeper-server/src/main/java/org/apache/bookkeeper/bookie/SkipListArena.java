/**
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
package org.apache.bookkeeper.bookie;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.conf.ServerConfiguration;

/**
 * SkipList allocation buffer to reduce memory fragment.
 * Adapted from HBase project.
 * <p>
 * The SkipListArena is basically a bump-the-pointer allocator that allocates
 * big (default 2MB) byte[] chunks from and then handles it out to threads that
 * request slices into the array.
 * </p>
 * <p>
 * The purpose of this class is to combat heap fragmentation in the
 * bookie. By ensuring that all KeyValues in a given SkipList refer
 * only to large chunks of contiguous memory, we ensure that large blocks
 * get freed up when the SkipList is flushed.
 * </p>
 * <p>
 * Without the Arena, the byte array allocated during insertion end up
 * interleaved throughout the heap, and the old generation gets progressively
 * more fragmented until a stop-the-world compacting collection occurs.
 * </p>
 */
public class SkipListArena {
    private AtomicReference<Chunk> curChunk = new AtomicReference<Chunk>();

    final int chunkSize;

    final int maxAlloc;

    public SkipListArena(ServerConfiguration cfg) {
        chunkSize = cfg.getSkipListArenaChunkSize();
        maxAlloc = cfg.getSkipListArenaMaxAllocSize();
        assert maxAlloc <= chunkSize;
    }

    /**
     * Allocate a slice of the given length.
     * <p>
     * If the size is larger than the maximum size specified for this allocator, returns null.
     * </p>
     */
    public MemorySlice allocateBytes(int size) {
        assert size >= 0;

        // Callers should satisfy large allocations directly from JVM since they
        // don't cause fragmentation as badly.
        if (size > maxAlloc) {
            return null;
        }

        while (true) {
            Chunk c = getCurrentChunk();

            // Try to allocate from this chunk
            int allocOffset = c.alloc(size);
            if (allocOffset != -1) {
                // We succeeded - this is the common case - small alloc
                // from a big buffer
                return new MemorySlice(c.data, allocOffset);
            }

            // not enough space!
            // try to retire this chunk
            retireCurrentChunk(c);
        }
    }

    /**
    * Try to retire the current chunk if it is still there.
    */
    private void retireCurrentChunk(Chunk c) {
        curChunk.compareAndSet(c, null);
        // If the CAS fails, that means that someone else already
        // retired the chunk for us.
    }

    /**
    * Get the current chunk, or, if there is no current chunk,
    * allocate a new one from the JVM.
    */
    private Chunk getCurrentChunk() {
        while (true) {
            // Try to get the chunk
            Chunk c = curChunk.get();
            if (c != null) {
                return c;
            }

            // No current chunk, so we want to allocate one. We race
            // against other allocators to CAS in an uninitialized chunk
            // (which is cheap to allocate)
            c = new Chunk(chunkSize);
            if (curChunk.compareAndSet(null, c)) {
                c.init();
                return c;
            }
            // lost race
        }
    }

    /**
    * A chunk of memory out of which allocations are sliced.
    */
    private static class Chunk {
        /** Actual underlying data. */
        private byte[] data;

        private static final int UNINITIALIZED = -1;
        private static final int OOM = -2;
        /**
         * Offset for the next allocation, or the sentinel value -1
         * which implies that the chunk is still uninitialized.
         */
        private AtomicInteger nextFreeOffset = new AtomicInteger(UNINITIALIZED);

        /** Total number of allocations satisfied from this buffer. */
        private AtomicInteger allocCount = new AtomicInteger();

        /** Size of chunk in bytes. */
        private final int size;

        /**
         * Create an uninitialized chunk. Note that memory is not allocated yet, so
         * this is cheap.
         * @param size in bytes
         */
        private Chunk(int size) {
            this.size = size;
        }

        /**
         * Actually claim the memory for this chunk. This should only be called from
         * the thread that constructed the chunk. It is thread-safe against other
         * threads calling alloc(), who will block until the allocation is complete.
         */
        public void init() {
            assert nextFreeOffset.get() == UNINITIALIZED;
            try {
                data = new byte[size];
            } catch (OutOfMemoryError e) {
                boolean failInit = nextFreeOffset.compareAndSet(UNINITIALIZED, OOM);
                assert failInit; // should be true.
                throw e;
            }
            // Mark that it's ready for use
            boolean okInit = nextFreeOffset.compareAndSet(UNINITIALIZED, 0);
            assert okInit;    // single-threaded call
        }

        /**
         * Try to allocate <code>size</code> bytes from the chunk.
         * @return the offset of the successful allocation, or -1 to indicate not-enough-space
         */
        public int alloc(int size) {
            while (true) {
                int oldOffset = nextFreeOffset.get();
                if (oldOffset == UNINITIALIZED) {
                    // Other thread allocating it right now
                    Thread.yield();
                    continue;
                }
                if (oldOffset == OOM) {
                    return -1;
                }

                if (oldOffset + size > data.length) {
                    return -1; // alloc doesn't fit
                }

                // Try to atomically claim this chunk
                if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size)) {
                    // we got the alloc
                    allocCount.incrementAndGet();
                    return oldOffset;
                }
                // lost race
            }
        }

        @Override
        public String toString() {
            return "Chunk@" + System.identityHashCode(this) + ": used(" + allocCount.get() + "), free("
                    + (data.length - nextFreeOffset.get() + ")");
        }
    }

    /**
    * The result of a single allocation. Contains the chunk that the
    * allocation points into, and the offset in this array where the
    * slice begins.
    */
    public static class MemorySlice {
        private final byte[] data;
        private final int offset;

        private MemorySlice(byte[] data, int off) {
            this.data = data;
            this.offset = off;
        }

        @Override
        public String toString() {
            return "Slice:" + "capacity(" + data.length + "), offset(" + offset + ")";
        }

        byte[] getData() {
            return data;
        }

        int getOffset() {
            return offset;
        }
    }
}
