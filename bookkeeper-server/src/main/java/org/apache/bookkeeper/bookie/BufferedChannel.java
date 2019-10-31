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
import io.netty.util.ReferenceCountUtil;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides a buffering layer in front of a FileChannel.
 */
public class BufferedChannel extends BufferedReadChannel implements Closeable {
    // The capacity of the write buffer.
    protected final int writeCapacity;
    // The position of the file channel's write pointer.
    protected AtomicLong writeBufferStartPosition = new AtomicLong(0);
    // The buffer used to write operations.
    protected final ByteBuf writeBuffer;
    // The absolute position of the next write operation.
    protected volatile long position;

    /*
     * if unpersistedBytesBound is non-zero value, then after writing to
     * writeBuffer, it will check if the unpersistedBytes is greater than
     * unpersistedBytesBound and then calls flush method if it is greater.
     *
     * It is a best-effort feature, since 'forceWrite' method is not
     * synchronized and unpersistedBytes is reset in 'forceWrite' method before
     * calling fileChannel.force
     */
    protected final long unpersistedBytesBound;
    private final boolean doRegularFlushes;

    /*
     * it tracks the number of bytes which are not persisted yet by force
     * writing the FileChannel. The unpersisted bytes could be in writeBuffer or
     * in fileChannel system cache.
     */
    protected final AtomicLong unpersistedBytes;

    private boolean closed = false;

    // make constructor to be public for unit test
    public BufferedChannel(ByteBufAllocator allocator, FileChannel fc, int capacity) throws IOException {
        // Use the same capacity for read and write buffers.
        this(allocator, fc, capacity, 0L);
    }

    public BufferedChannel(ByteBufAllocator allocator, FileChannel fc, int capacity, long unpersistedBytesBound)
            throws IOException {
        // Use the same capacity for read and write buffers.
        this(allocator, fc, capacity, capacity, unpersistedBytesBound);
    }

    public BufferedChannel(ByteBufAllocator allocator, FileChannel fc, int writeCapacity, int readCapacity,
            long unpersistedBytesBound) throws IOException {
        super(fc, readCapacity);
        this.writeCapacity = writeCapacity;
        this.position = fc.position();
        this.writeBufferStartPosition.set(position);
        this.writeBuffer = allocator.directBuffer(writeCapacity);
        this.unpersistedBytes = new AtomicLong(0);
        this.unpersistedBytesBound = unpersistedBytesBound;
        this.doRegularFlushes = unpersistedBytesBound > 0;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        ReferenceCountUtil.safeRelease(writeBuffer);
        fileChannel.close();
        closed = true;
    }

    /**
     * Write all the data in src to the {@link FileChannel}. Note that this function can
     * buffer or re-order writes based on the implementation. These writes will be flushed
     * to the disk only when flush() is invoked.
     *
     * @param src The source ByteBuffer which contains the data to be written.
     * @throws IOException if a write operation fails.
     */
    public void write(ByteBuf src) throws IOException {
        int copied = 0;
        boolean shouldForceWrite = false;
        synchronized (this) {
            int len = src.readableBytes();
            while (copied < len) {
                int bytesToCopy = Math.min(src.readableBytes() - copied, writeBuffer.writableBytes());
                writeBuffer.writeBytes(src, src.readerIndex() + copied, bytesToCopy);
                copied += bytesToCopy;

                // if we have run out of buffer space, we should flush to the
                // file
                if (!writeBuffer.isWritable()) {
                    flush();
                }
            }
            position += copied;
            if (doRegularFlushes) {
                unpersistedBytes.addAndGet(copied);
                if (unpersistedBytes.get() >= unpersistedBytesBound) {
                    flush();
                    shouldForceWrite = true;
                }
            }
        }
        if (shouldForceWrite) {
            forceWrite(false);
        }
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
     * calls both flush and forceWrite methods.
     *
     * @param forceMetadata
     *            - If true then this method is required to force changes to
     *            both the file's content and metadata to be written to storage;
     *            otherwise, it need only force content changes to be written
     * @throws IOException
     */
    public void flushAndForceWrite(boolean forceMetadata) throws IOException {
        flush();
        forceWrite(forceMetadata);
    }

    /**
     * calls both flush and forceWrite methods if regular flush is enabled.
     *
     * @param forceMetadata
     *            - If true then this method is required to force changes to
     *            both the file's content and metadata to be written to storage;
     *            otherwise, it need only force content changes to be written
     * @throws IOException
     */
    public void flushAndForceWriteIfRegularFlush(boolean forceMetadata) throws IOException {
        if (doRegularFlushes) {
            flushAndForceWrite(forceMetadata);
        }
    }

    /**
     * Write any data in the buffer to the file and advance the writeBufferPosition.
     * Callers are expected to synchronize appropriately
     *
     * @throws IOException if the write fails.
     */
    public synchronized void flush() throws IOException {
        ByteBuffer toWrite = writeBuffer.internalNioBuffer(0, writeBuffer.writerIndex());
        do {
            fileChannel.write(toWrite);
        } while (toWrite.hasRemaining());
        writeBuffer.clear();
        writeBufferStartPosition.set(fileChannel.position());
    }

    /*
     * force a sync operation so that data is persisted to the disk.
     */
    public long forceWrite(boolean forceMetadata) throws IOException {
        // This is the point up to which we had flushed to the file system page cache
        // before issuing this force write hence is guaranteed to be made durable by
        // the force write, any flush that happens after this may or may
        // not be flushed
        long positionForceWrite = writeBufferStartPosition.get();
        /*
         * since forceWrite method is not called in synchronized block, to make
         * sure we are not undercounting unpersistedBytes, setting
         * unpersistedBytes to the current number of bytes in writeBuffer.
         *
         * since we are calling fileChannel.force, bytes which are written to
         * filechannel (system filecache) will be persisted to the disk. So we
         * dont need to consider those bytes for setting value to
         * unpersistedBytes.
         *
         * In this method fileChannel.force is not called in synchronized block, so
         * we are doing best efforts to not overcount or undercount unpersistedBytes.
         * Hence setting writeBuffer.readableBytes() to unpersistedBytes.
         *
         */
        if (unpersistedBytesBound > 0) {
            synchronized (this) {
                unpersistedBytes.set(writeBuffer.readableBytes());
            }
        }

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

    public synchronized int getNumOfBytesInWriteBuffer() {
        return writeBuffer.readableBytes();
    }

    long getUnpersistedBytes() {
        return unpersistedBytes.get();
    }
}