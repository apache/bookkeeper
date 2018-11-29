package org.apache.bookkeeper.bookie;

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/**
 * Strictly for testing.
 * Have to be alongside with prod code for Journal to inject in tests.
 */
public class SlowBufferedChannel extends BufferedChannel {
    public volatile long getDelay = 0;
    public volatile long addDelay = 0;
    public volatile long flushDelay = 0;

    public SlowBufferedChannel(ByteBufAllocator allocator, FileChannel fc, int capacity) throws IOException {
        super(allocator, fc, capacity);
    }

    public SlowBufferedChannel(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity)
            throws IOException {
        super(allocator, fc, writeCapacity, readCapacity);
    }

    public void setAddDelay(long delay) {
        addDelay = delay;
    }

    public void setGetDelay(long delay) {
        getDelay = delay;
    }

    public void setFlushDelay(long delay) {
        flushDelay = delay;
    }

    @Override
    public synchronized void write(ByteBuf src) throws IOException {
        delayMs(addDelay);
        super.write(src);
    }

    @Override
    public void flush() throws IOException {
        delayMs(flushDelay);
        super.flush();
    }

    @Override
    public long forceWrite(boolean forceMetadata) throws IOException {
        delayMs(flushDelay);
        return super.forceWrite(forceMetadata);
    }

    @Override
    public synchronized int read(ByteBuf dest, long pos) throws IOException {
        delayMs(getDelay);
        return super.read(dest, pos);
    }

    private static void delayMs(long delay) {
        if (delay < 1) {
            return;
        }
        try {
            TimeUnit.MILLISECONDS.sleep(delay);
        } catch (InterruptedException e) {
            //noop
        }
    }
}
