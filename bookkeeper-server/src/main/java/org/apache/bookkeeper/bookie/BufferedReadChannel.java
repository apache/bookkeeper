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

package org.apache.bookkeeper.bookie;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * A Buffered channel without a write buffer. Only reads are buffered.
 */
public class BufferedReadChannel extends BufferedChannelBase  {

    // The capacity of the read buffer.
    protected final int readCapacity;

    // The buffer for read operations.
    protected final ByteBuf readBuffer;

    // The starting position of the data currently in the read buffer.
    protected long readBufferStartPosition = Long.MIN_VALUE;

    long invocationCount = 0;
    long cacheHitCount = 0;

    public BufferedReadChannel(FileChannel fileChannel, int readCapacity) {
        super(fileChannel);
        this.readCapacity = readCapacity;
        this.readBuffer = Unpooled.buffer(readCapacity);
    }

    /**
     * Read as many bytes into dest as dest.capacity() starting at position pos in the
     * FileChannel. This function can read from the buffer or the file channel
     * depending on the implementation..
     * @param dest
     * @param pos
     * @return The total number of bytes read.
     *         -1 if the given position is greater than or equal to the file's current size.
     * @throws IOException if I/O error occurs
     */
    public int read(ByteBuf dest, long pos) throws IOException {
        return read(dest, pos, dest.writableBytes());
    }

    public synchronized int read(ByteBuf dest, long pos, int length) throws IOException {
        invocationCount++;
        long currentPosition = pos;
        long eof = validateAndGetFileChannel().size();
        // return -1 if the given position is greater than or equal to the file's current size.
        if (pos >= eof) {
            return -1;
        }
        while (length > 0) {
            // Check if the data is in the buffer, if so, copy it.
            if (readBufferStartPosition <= currentPosition
                    && currentPosition < readBufferStartPosition + readBuffer.readableBytes()) {
                int posInBuffer = (int) (currentPosition - readBufferStartPosition);
                int bytesToCopy = Math.min(length, readBuffer.readableBytes() - posInBuffer);
                dest.writeBytes(readBuffer, posInBuffer, bytesToCopy);
                currentPosition += bytesToCopy;
                length -= bytesToCopy;
                cacheHitCount++;
            } else if (currentPosition >= eof) {
                // here we reached eof.
                break;
            } else {
                // We don't have it in the buffer, so put necessary data in the buffer
                readBufferStartPosition = currentPosition;
                int readBytes = 0;
                if ((readBytes = validateAndGetFileChannel().read(readBuffer.internalNioBuffer(0, readCapacity),
                        currentPosition)) <= 0) {
                    throw new IOException("Reading from filechannel returned a non-positive value. Short read.");
                }
                readBuffer.writerIndex(readBytes);
            }
        }
        return (int) (currentPosition - pos);
    }

    public synchronized void clear() {
        readBuffer.clear();
    }

}
