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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.bookkeeper.common.util.ExceptionMessageHelper.exMsg;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.common.util.nativeio.NativeIOException;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.commons.lang3.SystemUtils;

class DirectWriter implements LogWriter {
    final NativeIO nativeIO;
    final int fd;
    final int id;
    final String filename;
    final BufferPool bufferPool;
    final ExecutorService writeExecutor;
    final Object bufferLock = new Object();
    final List<Future<?>> outstandingWrites = new ArrayList<Future<?>>();
    final Slogger slog;
    Buffer nativeBuffer;
    long offset;
    private static volatile boolean useFallocate = true;

    DirectWriter(int id,
                 String filename,
                 long maxFileSize,
                 ExecutorService writeExecutor,
                 BufferPool bufferPool,
                 NativeIO nativeIO, Slogger slog) throws IOException {
        checkArgument(maxFileSize > 0, "Max file size (%d) must be positive");
        this.id = id;
        this.filename = filename;
        this.writeExecutor = writeExecutor;
        this.nativeIO = nativeIO;
        this.slog = slog.ctx(DirectWriter.class);

        offset = 0;

        try {
            fd = nativeIO.open(filename,
                               NativeIO.O_CREAT | NativeIO.O_WRONLY | NativeIO.O_DIRECT,
                               00644);
            checkState(fd >= 0, "Open should have thrown exception, fd is invalid : %d", fd);
        } catch (NativeIOException ne) {
            throw new IOException(exMsg(ne.getMessage()).kv("file", filename)
                                  .kv("errno", ne.getErrno()).toString(), ne);
        }

        if (useFallocate) {
            if (!SystemUtils.IS_OS_LINUX) {
                disableUseFallocate();
                this.slog.warn(Events.FALLOCATE_NOT_AVAILABLE);
            } else {
                try {
                    int ret = nativeIO.fallocate(fd, NativeIO.FALLOC_FL_ZERO_RANGE, 0, maxFileSize);
                    checkState(ret == 0, "Exception should have been thrown on non-zero ret: %d", ret);
                } catch (NativeIOException ex) {
                    // fallocate(2) is not supported on all filesystems.  Since this is an optimization, disable
                    // subsequent usage instead of failing the operation.
                    disableUseFallocate();
                    this.slog.kv("message", ex.getMessage())
                        .kv("file", filename)
                        .kv("errno", ex.getErrno())
                        .warn(Events.FALLOCATE_NOT_AVAILABLE);
                }
            }
        }

        this.bufferPool = bufferPool;
        this.nativeBuffer = bufferPool.acquire();
    }

    private static void disableUseFallocate() {
        DirectWriter.useFallocate = false;
    }

    @Override
    public int logId() {
        return id;
    }

    @Override
    public void writeAt(long offset, ByteBuf buf) throws IOException {
        checkArgument(Buffer.isAligned(offset),
                      "Offset to writeAt must be aligned to %d: %d is not", Buffer.ALIGNMENT, offset);
        checkArgument(Buffer.isAligned(buf.readableBytes()),
                      "Buffer must write multiple of alignment bytes (%d), %d is not",
                      Buffer.ALIGNMENT, buf.readableBytes());

        int bytesToWrite = buf.readableBytes();
        if (bytesToWrite <= 0) {
            return;
        }

        Buffer tmpBuffer = bufferPool.acquire();
        tmpBuffer.reset();
        tmpBuffer.writeByteBuf(buf);
        Future<?> f = writeExecutor.submit(() -> {
            writeByteBuf(tmpBuffer, bytesToWrite, offset);
            return null;
            });
        addOutstandingWrite(f);
    }

    private void writeByteBuf(Buffer buffer, int bytesToWrite, long offsetToWrite) throws IOException{
        try {
            if (bytesToWrite <= 0) {
                return;
            }
            int ret = nativeIO.pwrite(fd, buffer.pointer(), bytesToWrite, offsetToWrite);
            if (ret != bytesToWrite) {
                throw new IOException(exMsg("Incomplete write")
                    .kv("filename", filename)
                    .kv("pointer", buffer.pointer())
                    .kv("offset", offsetToWrite)
                    .kv("writeSize", bytesToWrite)
                    .kv("bytesWritten", ret)
                    .toString());
            }
        } catch (NativeIOException ne) {
            throw new IOException(exMsg("Write error")
                .kv("filename", filename)
                .kv("offset", offsetToWrite)
                .kv("writeSize", bytesToWrite)
                .kv("pointer", buffer.pointer())
                .kv("errno", ne.getErrno())
                .toString());
        } finally {
            bufferPool.release(buffer);
        }
    }

    @Override
    public int writeDelimited(ByteBuf buf) throws IOException {
        synchronized (bufferLock) {
            if (!nativeBuffer.hasSpace(serializedSize(buf))) {
                flushBuffer();
            }

            int readable = buf.readableBytes();
            long bufferPosition = position() + Integer.BYTES;
            if (bufferPosition > Integer.MAX_VALUE) {
                throw new IOException(exMsg("Cannot write past max int")
                                      .kv("filename", filename)
                                      .kv("writeSize", readable)
                                      .kv("position", bufferPosition)
                                      .toString());
            }
            nativeBuffer.writeInt(readable);
            nativeBuffer.writeByteBuf(buf);
            return (int) bufferPosition;
        }
    }

    @Override
    public void position(long offset) throws IOException {
        synchronized (bufferLock) {
            if (nativeBuffer != null && nativeBuffer.position() > 0) {
                flushBuffer();
            }
            if ((offset % Buffer.ALIGNMENT) != 0) {
                throw new IOException(exMsg("offset must be multiple of alignment")
                                      .kv("offset", offset)
                                      .kv("alignment", Buffer.ALIGNMENT)
                                      .toString());
            }
            this.offset = offset;
        }
    }

    @Override
    public long position() {
        synchronized (bufferLock) {
            return this.offset + (nativeBuffer != null ? nativeBuffer.position() : 0);
        }
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();

        waitForOutstandingWrites();

        try {
            int ret = nativeIO.fsync(fd);
            checkState(ret == 0, "Fsync should throw exception on non-zero return (%d)", ret);
        } catch (NativeIOException ne) {
            throw new IOException(exMsg(ne.getMessage())
                                  .kv("file", filename)
                                  .kv("errno", ne.getErrno()).toString());
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (bufferLock) {
            if (nativeBuffer != null && nativeBuffer.position() > 0) {
                flush();
            }
        }

        try {
            int ret = nativeIO.close(fd);
            checkState(ret == 0, "Close should throw exception on non-zero return (%d)", ret);
        } catch (NativeIOException ne) {
            throw new IOException(exMsg(ne.getMessage())
                                  .kv("file", filename)
                                  .kv("errno", ne.getErrno()).toString());
        } finally {
            synchronized (bufferLock) {
                bufferPool.release(nativeBuffer);
                nativeBuffer = null;
            }
        }
    }

    private void addOutstandingWrite(Future<?> toAdd) throws IOException {
        synchronized (outstandingWrites) {
            outstandingWrites.add(toAdd);

            Iterator<Future<?>> iter = outstandingWrites.iterator();
            while (iter.hasNext()) { // clear out completed futures
                Future<?> f = iter.next();
                if (f.isDone()) {
                    waitForFuture(f);
                    iter.remove();
                } else {
                    break;
                }
            }
        }
    }

    private void waitForOutstandingWrites() throws IOException {
        synchronized (outstandingWrites) {
            Iterator<Future<?>> iter = outstandingWrites.iterator();
            while (iter.hasNext()) { // clear out completed futures
                Future<?> f = iter.next();
                waitForFuture(f);
                iter.remove();
            }
        }
    }

    private void waitForFuture(Future<?> f) throws IOException {
        try {
            f.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException(ie);
        } catch (Throwable t) {
            if (t.getCause() instanceof IOException) {
                throw (IOException) t.getCause();
            } else {
                throw new IOException(t);
            }
        }
    }

    private void flushBuffer() throws IOException {
        synchronized (bufferLock) {
            if (this.nativeBuffer != null) {
                int bytesToWrite = this.nativeBuffer.padToAlignment();
                if (bytesToWrite == 0) {
                    return;
                }

                Buffer bufferToFlush = this.nativeBuffer;
                this.nativeBuffer = null;

                long offsetToWrite = offset;
                offset += bytesToWrite;

                Future<?> f = writeExecutor.submit(() -> {
                    writeByteBuf(bufferToFlush, bytesToWrite, offsetToWrite);
                    return null;
                });
                addOutstandingWrite(f);

                // must acquire after triggering the write
                // otherwise it could try to acquire a buffer without kicking off
                // a subroutine that will free another
                this.nativeBuffer = bufferPool.acquire();
            }
        }
    }

    @Override
    public int serializedSize(ByteBuf buf) {
        return buf.readableBytes() + Integer.BYTES;
    }
}
