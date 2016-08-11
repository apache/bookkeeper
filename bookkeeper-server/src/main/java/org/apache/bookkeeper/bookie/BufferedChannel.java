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
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides a buffering layer in front of a FileChannel.
 */
public class BufferedChannel extends BufferedReadChannel {
    // The capacity of the write buffer.
    protected final int writeCapacity;
    // The position of the file channel's write pointer.
    protected AtomicLong writeBufferStartPosition = new AtomicLong(0);
    // The buffer used to write operations.
    protected final ByteBuf writeBuffer;
    // The absolute position of the next write operation.
    protected volatile long position;

    // make constructor to be public for unit test
    public BufferedChannel(FileChannel fc, int capacity) throws IOException {
        // Use the same capacity for read and write buffers.
        this(fc, capacity, capacity);
    }

    public BufferedChannel(FileChannel fc, int writeCapacity, int readCapacity) throws IOException {
        super(fc, readCapacity);
        this.writeCapacity = writeCapacity;
        this.position = fc.position();
        this.writeBufferStartPosition.set(position);
        this.writeBuffer = ByteBufAllocator.DEFAULT.directBuffer(writeCapacity);
    }

    @Override
    public void close() throws IOException {
        super.close();
        writeBuffer.release();
    }

    /**
     * Write all the data in src to the {@link FileChannel}. Note that this function can
     * buffer or re-order writes based on the implementation. These writes will be flushed
     * to the disk only when flush() is invoked.
     *
     * @param src The source ByteBuffer which contains the data to be written.
     * @throws IOException if a write operation fails.
     */
    public synchronized void write(ByteBuf src) throws IOException {
        int copied = 0;
        int len = src.readableBytes();
        while (copied < len) {
            int bytesToCopy = Math.min(src.readableBytes() - copied, writeBuffer.writableBytes());
            writeBuffer.writeBytes(src, src.readerIndex() + copied, bytesToCopy);
            copied += bytesToCopy;

            // if we have run out of buffer space, we should flush to the file
            if (!writeBuffer.isWritable()) {
                flushInternal();
            }
        }
        position += copied;
    }

    /**
     * Get the position where the next write operation will begin writing from.
     * @return
     */
    public long position() {
        return position;
    }

    /**
     * Get the position of the file channel's write pointer.
     * @return
     */
    public long getFileChannelPosition() {
        return writeBufferStartPosition.get();
    }


    /**
     * Write any data in the buffer to the file. If sync is set to true, force a sync operation so that
     * data is persisted to the disk.
     * @param shouldForceWrite
     * @throws IOException if the write or sync operation fails.
     */
    public void flush(boolean shouldForceWrite) throws IOException {
        synchronized (this) {
            flushInternal();
        }
        if (shouldForceWrite) {
            forceWrite(false);
        }
    }

    /**
     * Write any data in the buffer to the file and advance the writeBufferPosition.
     * Callers are expected to synchronize appropriately
     * @throws IOException if the write fails.
     */
    private void flushInternal() throws IOException {
        ByteBuffer toWrite = writeBuffer.internalNioBuffer(0, writeBuffer.writerIndex());
        do {
            fileChannel.write(toWrite);
        } while (toWrite.hasRemaining());
        writeBuffer.clear();
        writeBufferStartPosition.set(fileChannel.position());
    }

    public long forceWrite(boolean forceMetadata) throws IOException {
        // This is the point up to which we had flushed to the file system page cache
        // before issuing this force write hence is guaranteed to be made durable by
        // the force write, any flush that happens after this may or may
        // not be flushed
        long positionForceWrite = writeBufferStartPosition.get();
        fileChannel.force(forceMetadata);
        return positionForceWrite;
    }

    @Override
    public synchronized int read(ByteBuf dest, long pos, int length) throws IOException {
        long prevPos = pos;
        while (length > 0) {
            // check if it is in the write buffer
            if (writeBuffer != null && writeBufferStartPosition.get() <= pos) {
                int positionInBuffer = (int) (pos - writeBufferStartPosition.get());
                int bytesToCopy = Math.min(writeBuffer.writerIndex() - positionInBuffer, dest.writableBytes());

                if (bytesToCopy == 0) {
                    throw new IOException("Read past EOF");
                }

                dest.writeBytes(writeBuffer, positionInBuffer, bytesToCopy);
                pos += bytesToCopy;
                length -= bytesToCopy;
            } else if (writeBuffer == null && writeBufferStartPosition.get() <= pos) {
                // here we reach the end
                break;
                // first check if there is anything we can grab from the readBuffer
            } else if (readBufferStartPosition <= pos && pos < readBufferStartPosition + readBuffer.writerIndex()) {
                int positionInBuffer = (int) (pos - readBufferStartPosition);
                int bytesToCopy = Math.min(readBuffer.writerIndex() - positionInBuffer, dest.writableBytes());
                dest.writeBytes(readBuffer, positionInBuffer, bytesToCopy);
                pos += bytesToCopy;
                length -= bytesToCopy;
                // let's read it
            } else {
                readBufferStartPosition = pos;

                int readBytes = fileChannel.read(readBuffer.internalNioBuffer(0, readCapacity),
                        readBufferStartPosition);
                if (readBytes <= 0) {
                    throw new IOException("Reading from filechannel returned a non-positive value. Short read.");
                }
                readBuffer.writerIndex(readBytes);
            }
        }
        return (int) (pos - prevPos);
    }

    @Override
    public synchronized void clear() {
        super.clear();
        writeBuffer.clear();
    }
}
