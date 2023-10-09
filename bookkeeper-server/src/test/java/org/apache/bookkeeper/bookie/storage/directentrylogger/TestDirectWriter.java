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

import static org.apache.bookkeeper.bookie.storage.directentrylogger.DirectEntryLogger.logFilename;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.common.util.nativeio.NativeIOException;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * TestDirectWriter.
 */
public class TestDirectWriter {
    private static final Slogger slog = Slogger.CONSOLE;

    private final TmpDirs tmpDirs = new TmpDirs();
    private final ExecutorService writeExecutor = Executors.newSingleThreadExecutor();

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
        writeExecutor.shutdownNow();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteAtAlignment() throws Exception {
        File ledgerDir = tmpDirs.createNew("writeAlignment", "logs");
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT, 8);
             LogWriter writer = new DirectWriter(5678, logFilename(ledgerDir, 5678),
                                                 1 << 24, writeExecutor,
                                                 buffers, new NativeIOImpl(), Slogger.CONSOLE)) {
            ByteBuf bb = Unpooled.buffer(Buffer.ALIGNMENT);
            TestBuffer.fillByteBuf(bb, 0xdededede);
            writer.writeAt(1234, bb);
            writer.flush();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteAlignmentSize() throws Exception {
        File ledgerDir = tmpDirs.createNew("writeAlignment", "logs");
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT, 8);
             LogWriter writer = new DirectWriter(5678, logFilename(ledgerDir, 5678), 1 << 24, writeExecutor,
                                                 buffers, new NativeIOImpl(), Slogger.CONSOLE)) {
            ByteBuf bb = Unpooled.buffer(123);
            TestBuffer.fillByteBuf(bb, 0xdededede);
            writer.writeAt(0, bb);
            writer.flush();
        }
    }

    @Test
    public void testWriteAlignedNotAtStart() throws Exception {
        File ledgerDir = tmpDirs.createNew("writeAlignment", "logs");
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT, 8);
             LogWriter writer = new DirectWriter(5678, logFilename(ledgerDir, 5678), 1 << 24, writeExecutor,
                                                 buffers, new NativeIOImpl(), Slogger.CONSOLE)) {
            ByteBuf bb = Unpooled.buffer(Buffer.ALIGNMENT);
            TestBuffer.fillByteBuf(bb, 0xdededede);
            writer.writeAt(Buffer.ALIGNMENT * 2, bb);
            writer.flush();
        }
    }


    @Test(timeout = 10000)
    public void testFlushingWillWaitForBuffer() throws Exception {
        File ledgerDir = tmpDirs.createNew("writeFailFailsFlush", "logs");
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT,
                Buffer.ALIGNMENT, 1); // only one buffer available, so we can't flush in bg
             LogWriter writer = new DirectWriter(5678, logFilename(ledgerDir, 5678), 1 << 24, writeExecutor,
                                                 buffers, new NativeIOImpl(), Slogger.CONSOLE)) {
            ByteBuf bb = Unpooled.buffer(Buffer.ALIGNMENT / 2);
            TestBuffer.fillByteBuf(bb, 0xdededede);
            writer.writeDelimited(bb);
            writer.flush();
        }
    }

    @Test(expected = IOException.class)
    public void testWriteFailFailsFlush() throws Exception {
        File ledgerDir = tmpDirs.createNew("writeFailFailsFlush", "logs");
        NativeIO io = new NativeIOImpl() {
                boolean failed = false;
                @Override
                public int pwrite(int fd, long pointer, int count, long offset) throws NativeIOException {
                    synchronized (this) {
                        if (!failed) {
                            failed = true;
                            throw new NativeIOException("fail for test");
                        }
                    }
                    return super.pwrite(fd, pointer, count, offset);
                }
            };
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT, 8);
             LogWriter writer = new DirectWriter(5678, logFilename(ledgerDir, 5678), 1 << 24, writeExecutor,
                                                 buffers, io, Slogger.CONSOLE)) {
            for (int i = 0; i < 10; i++) {
                ByteBuf bb = Unpooled.buffer(Buffer.ALIGNMENT / 2);
                TestBuffer.fillByteBuf(bb, 0xdededede);
                writer.writeDelimited(bb);
            }
            writer.flush();
        }
    }

    @Test(expected = IOException.class)
    public void testWriteAtFailFailsFlush() throws Exception {
        File ledgerDir = tmpDirs.createNew("writeFailFailsFlush", "logs");
        NativeIO io = new NativeIOImpl() {
                boolean failed = false;
                @Override
                public int pwrite(int fd, long pointer, int count, long offset) throws NativeIOException {
                    synchronized (this) {
                        if (!failed) {
                            failed = true;
                            throw new NativeIOException("fail for test");
                        }
                    }
                    return super.pwrite(fd, pointer, count, offset);
                }
            };

        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 1 << 14, 8);
             LogWriter writer = new DirectWriter(5678, logFilename(ledgerDir, 5678), 1 << 24, writeExecutor,
                                                 buffers, io, Slogger.CONSOLE)) {
            ByteBuf bb = Unpooled.buffer(Buffer.ALIGNMENT);
            TestBuffer.fillByteBuf(bb, 0xdededede);
            writer.writeAt(0, bb);
            writer.flush();
        }
    }

    @Test
    public void testWriteWithPadding() throws Exception {
        File ledgerDir = tmpDirs.createNew("paddingWrite", "logs");
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 1 << 14, 8);
             LogWriter writer = new DirectWriter(5678, logFilename(ledgerDir, 5678), 1 << 24, writeExecutor,
                                                 buffers, new NativeIOImpl(), Slogger.CONSOLE)) {
            ByteBuf bb = Unpooled.buffer(Buffer.ALIGNMENT);
            TestBuffer.fillByteBuf(bb, 0xdededede);
            bb.writerIndex(123);
            writer.writeDelimited(bb);
            writer.flush();
        }

        ByteBuf contents = readIntoByteBuf(ledgerDir, 5678);
        assertThat(contents.readInt(), equalTo(123));
        for (int i = 0; i < 123; i++) {
            assertThat(contents.readByte(), equalTo((byte) 0xde));
        }
        for (int i = 0; i < Buffer.ALIGNMENT - (123 + Integer.BYTES); i++) {
            assertThat(contents.readByte(), equalTo(Buffer.PADDING_BYTE));
        }
        while (contents.isReadable()) {
            assertThat((int) contents.readByte(), equalTo(0));
        }
    }

    @Test
    public void testWriteBlocksFlush() throws Exception {
        ExecutorService flushExecutor = Executors.newSingleThreadExecutor();
        try {
            File ledgerDir = tmpDirs.createNew("blockWrite", "logs");
            try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 1 << 14, 8);
                 LogWriter writer = new DirectWriter(1234, logFilename(ledgerDir, 1234),
                                                     1 << 24, writeExecutor,
                                                     buffers, new NativeIOImpl(), Slogger.CONSOLE)) {
                CompletableFuture<?> blocker = new CompletableFuture<>();
                writeExecutor.submit(() ->  {
                        blocker.join();
                        return null;
                    });
                ByteBuf bb = Unpooled.buffer(4096);
                TestBuffer.fillByteBuf(bb, 0xdeadbeef);
                writer.writeAt(0, bb);
                Future<?> f = flushExecutor.submit(() -> {
                        writer.flush();
                        return null;
                    });
                Thread.sleep(100);
                assertThat(f.isDone(), equalTo(false));
                blocker.complete(null);
                f.get();
            }
            ByteBuf contents = readIntoByteBuf(ledgerDir, 1234);
            for (int i = 0; i < 4096 / Integer.BYTES; i++) {
                assertThat(contents.readInt(), equalTo(0xdeadbeef));
            }
            if (contents.readableBytes() > 0) { // linux-only: fallocate will preallocate file
                while (contents.isReadable()) {
                    assertThat((int) contents.readByte(), equalTo(0));
                }
            }
        } finally {
            flushExecutor.shutdownNow();
        }
    }

    @Test(expected = IOException.class)
    public void testFailsToOpen() throws Exception {
        File ledgerDir = tmpDirs.createNew("failOpen", "logs");
        ledgerDir.delete();

        BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 1 << 14, 8);
        try {
            new DirectWriter(1234, logFilename(ledgerDir, 1234),
                    1 << 30, MoreExecutors.newDirectExecutorService(),
                    buffers, new NativeIOImpl(), Slogger.CONSOLE);
        } finally {
            buffers.close();
        }
    }

    @Test
    public void fallocateNotAvailable() throws Exception {
        File ledgerDir = tmpDirs.createNew("fallocUnavailable", "logs");
        NativeIO nativeIO = new NativeIOImpl() {
                @Override
                public int fallocate(int fd, int mode, long offset, long len)
                        throws NativeIOException {
                    throw new NativeIOException("pretending I'm a mac");
                }
            };
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 1 << 14, 8);
             LogWriter writer = new DirectWriter(3456, logFilename(ledgerDir, 3456),
                                                 1 << 24, writeExecutor,
                                                 buffers, nativeIO, Slogger.CONSOLE)) {
            ByteBuf bb = Unpooled.buffer(Buffer.ALIGNMENT);
            TestBuffer.fillByteBuf(bb, 0xdeadbeef);

            writer.writeAt(0, bb);
            writer.flush();
        }

        // should be 0xdeadbeef until the end of the file
        ByteBuf contents = readIntoByteBuf(ledgerDir, 3456);
        assertThat(contents.readableBytes(), equalTo(Buffer.ALIGNMENT));
        while (contents.isReadable()) {
            assertThat(contents.readInt(), equalTo(0xdeadbeef));
        }
    }

    @Test
    public void testWriteAtIntLimit() throws Exception {
        File ledgerDir = tmpDirs.createNew("intLimit", "logs");

        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, 1 << 14, 8);
             LogWriter writer = new DirectWriter(3456, logFilename(ledgerDir, 3456),
                                                 (long) Integer.MAX_VALUE + (Buffer.ALIGNMENT * 100),
                                                 writeExecutor,
                                                 buffers, new NativeIOImpl(), Slogger.CONSOLE)) {
            ByteBuf b1 = Unpooled.buffer(Buffer.ALIGNMENT - (Integer.BYTES * 2) - 1);
            TestBuffer.fillByteBuf(b1, 0xdeadbeef);

            long finalSeekablePosition = Integer.MAX_VALUE & ~(Buffer.ALIGNMENT - 1);
            writer.position(finalSeekablePosition);
            long offset = writer.writeDelimited(b1);
            assertThat(offset, equalTo(finalSeekablePosition + Integer.BYTES));
            assertThat(writer.position(), equalTo((long) Integer.MAX_VALUE - Integer.BYTES));

            offset = writer.writeDelimited(b1);
            assertThat(offset, equalTo((long) Integer.MAX_VALUE));

            writer.flush();

            try {
                writer.writeDelimited(b1);
                Assert.fail("Shouldn't be possible, we've gone past MAX_INT");
            } catch (IOException ioe) {
                // expected
            }
        }

    }

    static ByteBuf readIntoByteBuf(File directory, int logId) throws Exception {
        byte[] bytes = new byte[1024];
        File file = new File(DirectEntryLogger.logFilename(directory, logId));
        slog.kv("filename", file.toString()).info("reading in");
        ByteBuf byteBuf = Unpooled.buffer((int) file.length());
        try (FileInputStream is = new FileInputStream(file)) {
            int bytesRead = is.read(bytes);
            while (bytesRead > 0) {
                byteBuf.writeBytes(bytes, 0, bytesRead);
                bytesRead = is.read(bytes);
            }
        }

        assertThat(byteBuf.readableBytes(), equalTo((int) file.length()));
        return byteBuf;
    }
}
