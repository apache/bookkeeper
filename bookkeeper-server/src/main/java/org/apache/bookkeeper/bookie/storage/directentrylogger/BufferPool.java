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
package org.apache.bookkeeper.bookie.storage.directentrylogger;

import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;

/**
 * BufferPool used to manage Buffers.
 */
public class BufferPool implements AutoCloseable {
    private final ArrayBlockingQueue<Buffer> pool;

    BufferPool(NativeIO nativeIO, ByteBufAllocator allocator, int bufferSize, int maxPoolSize) throws IOException {
        pool = new ArrayBlockingQueue<>(maxPoolSize);
        for (int i = 0; i < maxPoolSize; i++) {
            pool.add(new Buffer(nativeIO, allocator, bufferSize));
        }
    }

    Buffer acquire() throws IOException {
        try {
            return pool.take();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException(ie);
        }
    }

    void release(Buffer buffer) {
        buffer.reset();
        if (!pool.add(buffer)) {
            buffer.free();
        }
    }

    @Override
    public void close() {
        while (true) {
            Buffer b = pool.poll();
            if (b == null) {
                break;
            }

            b.free();
        }
    }
}
