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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.bookkeeper.common.util.ExceptionMessageHelper.exMsg;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.common.util.nativeio.NativeIOException;
import org.apache.bookkeeper.stats.OpStatsLogger;

class DirectReader implements LogReader {
    private final ByteBufAllocator allocator;
    private final NativeIO nativeIO;
    private final Buffer nativeBuffer;
    private final String filename;
    private final int logId;
    private final int fd;
    private final int maxSaneEntrySize;
    private final OpStatsLogger readBlockStats;
    private long currentBlock = -1;
    private long currentBlockEnd = -1;
    private long maxOffset;
    private boolean closed;

    DirectReader(int logId, String filename, ByteBufAllocator allocator,
                 NativeIO nativeIO, int bufferSize,
                 int maxSaneEntrySize, OpStatsLogger readBlockStats) throws IOException {
        this.nativeIO = nativeIO;
        this.allocator = allocator;
        this.logId = logId;
        this.filename = filename;
        this.maxSaneEntrySize = maxSaneEntrySize;
        this.readBlockStats = readBlockStats;
        closed = false;

        try {
            fd = nativeIO.open(filename,
                               NativeIO.O_RDONLY | NativeIO.O_DIRECT,
                               00755);
            checkState(fd >= 0, "Open should throw exception on negative return (%d)", fd);
        } catch (NativeIOException ne) {
            throw new IOException(exMsg(ne.getMessage())
                                  .kv("file", filename)
                                  .kv("errno", ne.getErrno()).toString());
        }
        refreshMaxOffset();
        nativeBuffer = new Buffer(nativeIO, allocator, bufferSize);
    }

    @Override
    public int logId() {
        return logId;
    }

    private void clearCache() {
        synchronized (nativeBuffer) {
            currentBlock = -1;
            currentBlockEnd = -1;
        }
    }

    @Override
    public ByteBuf readBufferAt(long offset, int size) throws IOException, EOFException {
        ByteBuf buf = allocator.buffer(size);
        try {
            readIntoBufferAt(buf, offset, size);
        } catch (IOException e) {
            ReferenceCountUtil.release(buf);
            throw e;
        }

        return buf;
    }

    @Override
    public void readIntoBufferAt(ByteBuf buffer, long offset, int size) throws IOException, EOFException {
        assertValidOffset(offset);
        synchronized (nativeBuffer) {
            while (size > 0) {
                int bytesRead = readBytesIntoBuf(buffer, offset, size);
                size -= bytesRead;
                offset += bytesRead;
            }
        }
    }

    @Override
    public int readIntAt(long offset) throws IOException, EOFException {
        assertValidOffset(offset);
        synchronized (nativeBuffer) {
            if (offset >= currentBlock && offset + Integer.BYTES <= currentBlockEnd) { // fast path
                return nativeBuffer.readInt(offsetInBlock(offset));
            } else { // slow path
                ByteBuf intBuf = readBufferAt(offset, Integer.BYTES);
                try {
                    return intBuf.getInt(0);
                } finally {
                    ReferenceCountUtil.release(intBuf);
                }
            }
        }
    }

    @Override
    public long readLongAt(long offset) throws IOException, EOFException {
        assertValidOffset(offset);
        synchronized (nativeBuffer) {
            if (offset >= currentBlock && offset + Long.BYTES <= currentBlockEnd) { // fast path
                return nativeBuffer.readLong(offsetInBlock(offset));
            } else { // slow path
                ByteBuf longBuf = readBufferAt(offset, Long.BYTES);
                try {
                    return longBuf.getLong(0);
                } finally {
                    ReferenceCountUtil.release(longBuf);
                }
            }
        }
    }

    private int readBytesIntoBuf(ByteBuf buf, long offset, int size) throws IOException, EOFException {
        synchronized (nativeBuffer) {
            if (offset < currentBlock || offset >= currentBlockEnd) {
                readBlock(offset);
            }
            int offsetInBuffer = offsetInBlock(offset);
            int sizeInBuffer = sizeInBlock(offset, size);
            if (sizeInBuffer <= 0) {
                throw new EOFException(exMsg("Not enough bytes available")
                                      .kv("file", filename)
                                      .kv("fileSize", maxOffset)
                                      .kv("offset", offset)
                                      .kv("size", size).toString());
            }
            return nativeBuffer.readByteBuf(buf, offsetInBuffer, size);
        }
    }

    @Override
    public ByteBuf readEntryAt(int offset) throws IOException, EOFException {
        assertValidEntryOffset(offset);
        int sizeOffset = offset - Integer.BYTES;
        if (sizeOffset < 0) {
            throw new IOException(exMsg("Invalid offset, buffer size missing")
                                  .kv("file", filename)
                                  .kv("offset", offset).toString());
        }

        int entrySize = readIntAt(sizeOffset);
        if (entrySize == 0) {
            // reading an entry with size 0 may mean reading from preallocated
            // space. if we receive an offset in preallocated space, it may
            // mean that a write has occurred and been flushed, but our view
            // of that block is out of date. So clear the cache and let it be
            // loaded again.
            clearCache();
            entrySize = readIntAt(sizeOffset);
        }
        if (entrySize > maxSaneEntrySize || entrySize <= 0) {
            throw new IOException(exMsg("Invalid entry size")
                                  .kv("file", filename)
                                  .kv("offset", offset)
                                  .kv("maxSaneEntrySize", maxSaneEntrySize)
                                  .kv("readEntrySize", entrySize).toString());
        }
        return readBufferAt(offset, entrySize);
    }

    void readBlock(long offset) throws IOException {
        final int blockSize = nativeBuffer.size();
        assertValidBlockSize(blockSize);
        final long blockStart = offset & ~(blockSize - 1);

        if (blockStart + blockSize > maxOffset) {
            // Check if there's new data in the file
            refreshMaxOffset();
        }
        final long bytesAvailable = maxOffset > blockStart ? maxOffset - blockStart : 0;
        final long startNs = System.nanoTime();

        long bufferOffset = 0;
        long bytesToRead = Math.min(blockSize, bytesAvailable);
        long bytesOutstanding = bytesToRead;
        long bytesRead = -1;
        try {
            while (true) {
                long readSize = blockSize - bufferOffset;
                long pointerWithOffset = nativeBuffer.pointer(bufferOffset, readSize);
                bytesRead = nativeIO.pread(fd, pointerWithOffset,
                                           readSize,
                                           blockStart + bufferOffset);
                // offsets and counts must be aligned, so ensure that if we
                // get a short read, we don't throw off the alignment. For example
                // if we're trying to read 12K and we only managed 100 bytes,
                // we don't progress the offset or outstanding at all. However, if we
                // read 4196 bytes, we can progress the offset by 4KB and the outstanding
                // bytes will then be 100.
                // the only non-short read that isn't aligned is the bytes at the end of
                // of the file, which is why we don't align before we check if we should
                // exit the loop
                if ((bytesOutstanding - bytesRead) <= 0) {
                    break;
                }
                bytesOutstanding -= bytesRead & Buffer.ALIGNMENT;
                bufferOffset += bytesRead & Buffer.ALIGNMENT;
            }
        } catch (NativeIOException ne) {
            readBlockStats.registerFailedEvent(System.nanoTime() - startNs, TimeUnit.NANOSECONDS);
            throw new IOException(exMsg(ne.getMessage())
                                  .kv("requestedBytes", blockSize)
                                  .kv("offset", blockStart)
                                  .kv("expectedBytes", Math.min(blockSize, bytesAvailable))
                                  .kv("bytesOutstanding", bytesOutstanding)
                                  .kv("bufferOffset", bufferOffset)
                                  .kv("file", filename)
                                  .kv("fd", fd)
                                  .kv("errno", ne.getErrno()).toString());
        }
        readBlockStats.registerSuccessfulEvent(System.nanoTime() - startNs, TimeUnit.NANOSECONDS);
        currentBlock = blockStart;
        currentBlockEnd = blockStart + Math.min(blockSize, bytesAvailable);
    }

    @Override
    public void close() throws IOException {
        synchronized (nativeBuffer) {
            nativeBuffer.free();
        }

        try {
            int ret = nativeIO.close(fd);
            checkState(ret == 0, "Close should throw exception on non-zero return (%d)", ret);
            closed = true;
        } catch (NativeIOException ne) {
            throw new IOException(exMsg(ne.getMessage())
                    .kv("file", filename)
                    .kv("errno", ne.getErrno()).toString());
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public long maxOffset() {
        return maxOffset;
    }

    private void refreshMaxOffset() throws IOException {
        try {
            long ret = nativeIO.lseek(fd, 0, NativeIO.SEEK_END);
            checkState(ret >= 0,
                                     "Lseek should throw exception on negative return (%d)", ret);
            synchronized (this) {
                maxOffset = ret;
            }
        } catch (NativeIOException ne) {
            throw new IOException(exMsg(ne.getMessage())
                                  .kv("file", filename)
                                  .kv("fd", fd)
                                  .kv("errno", ne.getErrno()).toString());
        }
    }

    private int offsetInBlock(long offset) {
        long blockOffset = offset - currentBlock;
        if (blockOffset < 0 || blockOffset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(exMsg("Invalid offset passed")
                                               .kv("offset", offset).kv("blockOffset", blockOffset)
                                               .kv("currentBlock", currentBlock).toString());
        }
        return (int) blockOffset;
    }

    private int sizeInBlock(long offset, int size) {
        if (offset > currentBlockEnd || offset < currentBlock) {
            throw new IllegalArgumentException(exMsg("Invalid offset passed")
                                               .kv("offset", offset)
                                               .kv("currentBlock", currentBlock)
                                               .kv("currentBlockEnd", currentBlockEnd).toString());
        }

        long available = currentBlockEnd - offset;
        checkState(available <= Integer.MAX_VALUE, "Available(%d) must be less than max int", available);
        return Math.min(size, (int) available);
    }

    private static void assertValidOffset(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException(
                    exMsg("Offset can't be negative").kv("offset", offset).toString());
        }
    }

    private static void assertValidEntryOffset(long offset) {
        assertValidOffset(offset);
        if (offset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    exMsg("Entry offset must be less than max int").kv("offset", offset).toString());
        }
    }

    private static void assertValidBlockSize(int blockSize) {
        boolean valid = blockSize > 0 && Buffer.isAligned(blockSize);
        if (!valid) {
            throw new IllegalArgumentException(
                    exMsg("Invalid block size, must be power of 2")
                    .kv("blockSize", blockSize)
                    .kv("minBlockSize", Buffer.ALIGNMENT).toString());
        }
    }
}
