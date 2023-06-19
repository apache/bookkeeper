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
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.bookkeeper.common.util.nativeio.NativeIOException;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;
import org.apache.bookkeeper.slogger.Slogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.test.TmpDirs;
import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;


/**
 * TestDirectReader.
 */
public class TestDirectReader {

    private final TmpDirs tmpDirs = new TmpDirs();
    private final ExecutorService writeExecutor = Executors.newSingleThreadExecutor();
    private final OpStatsLogger opLogger = NullStatsLogger.INSTANCE.getOpStatsLogger("null");

    @Before
    public void before() {
        Assume.assumeFalse(SystemUtils.IS_OS_WINDOWS);
    }

    @After
    public void cleanup() throws Exception {
        tmpDirs.cleanup();
        writeExecutor.shutdownNow();
    }

    @Test
    public void testReadInt() throws Exception {
        File ledgerDir = tmpDirs.createNew("readInt", "logs");

        writeFileWithPattern(ledgerDir, 1234, 0xdeadbeef, 0, 1 << 20);

        try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT,
                                                 1 << 20, opLogger)) {
            assertThat(reader.readIntAt(0), equalTo(0xdeadbeef));
            assertThat(reader.readIntAt(2), equalTo(0xbeefdead));
            assertThat(reader.readIntAt(1024), equalTo(0xdeadbeef));
            assertThat(reader.readIntAt(1025), equalTo(0xadbeefde));
        }
    }

    @Test
    public void testReadIntAcrossBoundary() throws Exception {
        File ledgerDir = tmpDirs.createNew("readInt", "logs");

        writeFileWithPattern(ledgerDir, 1234, 0xdeadbeef, 0, 1 << 20);

        try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT,
                                                 1 << 20, opLogger)) {
            assertThat(reader.readIntAt(Buffer.ALIGNMENT - 2), equalTo(0xbeefdead));
        }
    }

    @Test
    public void testReadLong() throws Exception {
        File ledgerDir = tmpDirs.createNew("readLong", "logs");

        writeFileWithPattern(ledgerDir, 1234, 0xbeefcafe, 0, 1 << 20);

        try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT,
                                                 1 << 20, opLogger)) {
            assertThat(reader.readLongAt(0), equalTo(0xbeefcafebeefcafeL));
            assertThat(reader.readLongAt(2), equalTo(0xcafebeefcafebeefL));
            assertThat(reader.readLongAt(1024), equalTo(0xbeefcafebeefcafeL));
            assertThat(reader.readLongAt(1025), equalTo(0xefcafebeefcafebeL));
        }
    }

    @Test
    public void testReadLongAcrossBoundary() throws Exception {
        File ledgerDir = tmpDirs.createNew("readLong", "logs");

        writeFileWithPattern(ledgerDir, 1234, 0xbeefcafe, 0, 1 << 20);

        try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT,
                                                 1 << 20, opLogger)) {
            assertThat(reader.readLongAt(0), equalTo(0xbeefcafebeefcafeL));
            assertThat(reader.readLongAt(2), equalTo(0xcafebeefcafebeefL));
            assertThat(reader.readLongAt(1024), equalTo(0xbeefcafebeefcafeL));
            assertThat(reader.readLongAt(1025), equalTo(0xefcafebeefcafebeL));
        }
    }

    @Test
    public void testReadBuffer() throws Exception {
        File ledgerDir = tmpDirs.createNew("readBuffer", "logs");

        writeFileWithPattern(ledgerDir, 1234, 0xbeefcafe, 1, 1 << 20);

        try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT * 4,
                                                 1 << 20, opLogger)) {
            ByteBuf bb = reader.readBufferAt(0, Buffer.ALIGNMENT * 2);
            try {
                for (int j = 0; j < Buffer.ALIGNMENT / Integer.BYTES; j++) {
                    assertThat(bb.readInt(), equalTo(0xbeefcafe));
                }
                for (int i = 0; i < Buffer.ALIGNMENT / Integer.BYTES; i++) {
                    assertThat(bb.readInt(), equalTo(0xbeefcaff));
                }
                assertThat(bb.readableBytes(), equalTo(0));
            } finally {
                bb.release();
            }

            bb = reader.readBufferAt(Buffer.ALIGNMENT * 8, Buffer.ALIGNMENT);
            try {
                for (int j = 0; j < Buffer.ALIGNMENT / Integer.BYTES; j++) {
                    assertThat(bb.readInt(), equalTo(0xbeefcb06));
                }
                assertThat(bb.readableBytes(), equalTo(0));
            } finally {
                bb.release();
            }

            bb = reader.readBufferAt(Buffer.ALIGNMENT * 10 + 123, 345);
            try {
                assertThat(bb.readByte(), equalTo((byte) 0x08));
                for (int j = 0; j < 344 / Integer.BYTES; j++) {
                    assertThat(bb.readInt(), equalTo(0xbeefcb08));
                }
                assertThat(bb.readableBytes(), equalTo(0));
            } finally {
                bb.release();
            }

        }
    }

    @Test
    public void testReadBufferAcrossBoundary() throws Exception {
        File ledgerDir = tmpDirs.createNew("readBuffer", "logs");

        writeFileWithPattern(ledgerDir, 1234, 0xbeefcafe, 1, 1 << 20);
        BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT * 4, 8);

        try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT * 4,
                                                 1 << 20, opLogger)) {
            ByteBuf bb = reader.readBufferAt((long) (Buffer.ALIGNMENT * 3.5), Buffer.ALIGNMENT);
            try {
                for (int j = 0; j < (Buffer.ALIGNMENT / Integer.BYTES) / 2; j++) {
                    assertThat(bb.readInt(), equalTo(0xbeefcb01));
                }
                for (int i = 0; i < (Buffer.ALIGNMENT / Integer.BYTES) / 2; i++) {
                    assertThat(bb.readInt(), equalTo(0xbeefcb02));
                }
                assertThat(bb.readableBytes(), equalTo(0));
            } finally {
                bb.release();
            }
        }
    }

    @Test
    public void testReadBufferBiggerThanReaderBuffer() throws Exception {
        File ledgerDir = tmpDirs.createNew("readBuffer", "logs");

        writeFileWithPattern(ledgerDir, 1234, 0xbeefcafe, 1, 1 << 20);

        // buffer size is ALIGNMENT, read will be ALIGNMENT*2
        try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT,
                                                 1 << 20, opLogger)) {
            ByteBuf bb = reader.readBufferAt(0, Buffer.ALIGNMENT * 2);
            try {
                for (int j = 0; j < Buffer.ALIGNMENT / Integer.BYTES; j++) {
                    assertThat(bb.readInt(), equalTo(0xbeefcafe));
                }
                for (int i = 0; i < Buffer.ALIGNMENT / Integer.BYTES; i++) {
                    assertThat(bb.readInt(), equalTo(0xbeefcaff));
                }
                assertThat(bb.readableBytes(), equalTo(0));
            } finally {
                bb.release();
            }
        }
    }

    @Test(expected = EOFException.class)
    public void testReadPastEndOfFile() throws Exception {
        File ledgerDir = tmpDirs.createNew("readBuffer", "logs");

        writeFileWithPattern(ledgerDir, 1234, 0xbeeeeeef, 1, 1 << 13);
        try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT,
                                                 1 << 20, opLogger)) {
            reader.readBufferAt(1 << 13, Buffer.ALIGNMENT);
        }
    }

    @Test(expected = EOFException.class)
    public void testReadPastEndOfFilePartial() throws Exception {
        File ledgerDir = tmpDirs.createNew("readBuffer", "logs");

        writeFileWithPattern(ledgerDir, 1234, 0xbeeeeeef, 1, 1 << 13);
        try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT,
                                                 1 << 20, opLogger)) {
            reader.readBufferAt((1 << 13) - Buffer.ALIGNMENT / 2, Buffer.ALIGNMENT);
        }
    }

    @Test
    public void testReadEntries() throws Exception {
        File ledgerDir = tmpDirs.createNew("readEntries", "logs");

        int entrySize = Buffer.ALIGNMENT / 4 + 100;
        Map<Integer, Integer> offset2Pattern = new HashMap<>();
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT, 8);
             LogWriter writer = new DirectWriter(1234, logFilename(ledgerDir, 1234),
                                                 1 << 20, MoreExecutors.newDirectExecutorService(),
                                                 buffers, new NativeIOImpl(), Slogger.CONSOLE)) {
            for (int i = 0; i < 1000; i++) {
                ByteBuf bb = Unpooled.buffer(entrySize);
                int pattern = 0xbeef + i;
                TestBuffer.fillByteBuf(bb, pattern);
                int offset = writer.writeDelimited(bb);
                offset2Pattern.put(offset, pattern);
            }
        }

        try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT,
                                                 1 << 20, opLogger)) {
            List<Map.Entry<Integer, Integer>> offset2PatternList =
                new ArrayList<Map.Entry<Integer, Integer>>(offset2Pattern.entrySet());
            Collections.shuffle(offset2PatternList);

            for (Map.Entry<Integer, Integer> e : offset2PatternList) {
                ByteBuf entry = reader.readEntryAt(e.getKey());
                try {
                    assertThat(entry.readableBytes(), equalTo(entrySize));
                    while (entry.isReadable()) {
                        assertThat(entry.readInt(), equalTo(e.getValue()));
                    }
                } finally {
                    entry.release();
                }
            }
        }
    }

    @Test
    public void testReadFromFileBeingWrittenNoPreallocation() throws Exception {
        File ledgerDir = tmpDirs.createNew("readWhileWriting", "logs");

        int entrySize = Buffer.ALIGNMENT / 2 + 8;
        NativeIOImpl nativeIO = new NativeIOImpl() {
                @Override
                public int fallocate(int fd, int mode, long offset, long len)
                        throws NativeIOException {
                    return 0;
                }
            };
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT, 8);
             LogWriter writer = new DirectWriter(1234, logFilename(ledgerDir, 1234),
                                                 1 << 20, MoreExecutors.newDirectExecutorService(),
                                                 buffers, new NativeIOImpl(), Slogger.CONSOLE);
             LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT,
                                                 1 << 20, opLogger)) {
            ByteBuf b2 = Unpooled.buffer(entrySize);
            TestBuffer.fillByteBuf(b2, 0xfede);
            int offset = writer.writeDelimited(b2);

            try {
                reader.readEntryAt(offset);
                Assert.fail("Should have failed");
            } catch (IOException ioe) {
                // expected
            }
            writer.flush();

            ByteBuf bbread = reader.readEntryAt(offset);
            try {
                assertThat(bbread.readableBytes(), equalTo(entrySize));
                while (bbread.isReadable()) {
                    assertThat(bbread.readInt(), equalTo(0xfede));
                }
            } finally {
                bbread.release();
            }
        }
    }

    @Test
    public void testReadFromFileBeingWrittenReadInPreallocated() throws Exception {
        File ledgerDir = tmpDirs.createNew("readWhileWriting", "logs");

        int entrySize = Buffer.ALIGNMENT / 2 + 8;

        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT, 8);
             LogWriter writer = new DirectWriter(1234, logFilename(ledgerDir, 1234),
                                                 1 << 20, MoreExecutors.newDirectExecutorService(),
                                                 buffers, new NativeIOImpl(), Slogger.CONSOLE);
             LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT,
                                                 1 << 20, opLogger)) {
            ByteBuf bb = Unpooled.buffer(entrySize);
            TestBuffer.fillByteBuf(bb, 0xfeed);
            int offset = writer.writeDelimited(bb);

            try {
                reader.readEntryAt(offset);
                Assert.fail("Should have failed");
            } catch (IOException ioe) {
                // expected
            }
            writer.flush();
            ByteBuf bbread = reader.readEntryAt(offset);
            try {
                assertThat(bbread.readableBytes(), equalTo(entrySize));
                while (bbread.isReadable()) {
                    assertThat(bbread.readInt(), equalTo(0xfeed));
                }
            } finally {
                bbread.release();
            }
        }
    }

    @Test
    public void testPartialRead() throws Exception {
        File ledgerDir = tmpDirs.createNew("partialRead", "logs");

        int entrySize = Buffer.ALIGNMENT * 4;

        NativeIOImpl nativeIO = new NativeIOImpl() {
                @Override
                public long pread(int fd, long buf, long size, long offset) throws NativeIOException {
                    long read = super.pread(fd, buf, size, offset);
                    return Math.min(read, Buffer.ALIGNMENT); // force only less than a buffer read
                }

                @Override
                public int fallocate(int fd, int mode, long offset, long len)
                        throws NativeIOException {
                    return 0; // don't preallocate
                }
            };
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(),
            ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT * 10, 8);
             LogWriter writer = new DirectWriter(1234, logFilename(ledgerDir, 1234), 1 << 20,
                                                 MoreExecutors.newDirectExecutorService(),
                                                 buffers, new NativeIOImpl(), Slogger.CONSOLE)) {
            ByteBuf b1 = Unpooled.buffer(entrySize);
            TestBuffer.fillByteBuf(b1, 0xfeedfeed);
            int offset1 = writer.writeDelimited(b1);

            ByteBuf b2 = Unpooled.buffer(entrySize);
            TestBuffer.fillByteBuf(b2, 0xfedefede);
            int offset2 = writer.writeDelimited(b2);
            writer.flush();

            try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                     ByteBufAllocator.DEFAULT,
                                                     nativeIO, Buffer.ALIGNMENT * 3,
                                                     1 << 20, opLogger)) {
                ByteBuf bbread = reader.readEntryAt(offset1);
                try {
                    assertThat(bbread.readableBytes(), equalTo(entrySize));
                    while (bbread.readableBytes() >= Integer.BYTES) {
                        assertThat(bbread.readInt(), equalTo(0xfeedfeed));
                    }
                    assertThat(bbread.readableBytes(), equalTo(0));
                } finally {
                    bbread.release();
                }

                bbread = reader.readEntryAt(offset2);
                try {
                    assertThat(bbread.readableBytes(), equalTo(entrySize));
                    while (bbread.readableBytes() >= Integer.BYTES) {
                        assertThat(bbread.readInt(), equalTo(0xfedefede));
                    }
                    assertThat(bbread.readableBytes(), equalTo(0));
                } finally {
                    bbread.release();
                }
            }
        }
    }

    @Test
    public void testLargeEntry() throws Exception {
        File ledgerDir = tmpDirs.createNew("largeEntries", "logs");

        int entrySize = Buffer.ALIGNMENT * 4;

        int offset1, offset2;
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT * 8, 8);
             LogWriter writer = new DirectWriter(1234, logFilename(ledgerDir, 1234), 1 << 20,
                                                 MoreExecutors.newDirectExecutorService(), buffers, new NativeIOImpl(),
                                                 Slogger.CONSOLE)) {
            ByteBuf b1 = Unpooled.buffer(entrySize);
            TestBuffer.fillByteBuf(b1, 0xfeedfeed);
            offset1 = writer.writeDelimited(b1);

            ByteBuf b2 = Unpooled.buffer(entrySize);
            TestBuffer.fillByteBuf(b2, 0xfedefede);
            offset2 = writer.writeDelimited(b2);
            writer.flush();
        }

        try (LogReader reader = new DirectReader(1234, logFilename(ledgerDir, 1234),
                                                 ByteBufAllocator.DEFAULT,
                                                 new NativeIOImpl(), Buffer.ALIGNMENT,
                                                 1 << 20, opLogger)) {
            ByteBuf bbread = reader.readEntryAt(offset1);
            try {
                assertThat(bbread.readableBytes(), equalTo(entrySize));
                while (bbread.readableBytes() >= Integer.BYTES) {
                    assertThat(bbread.readInt(), equalTo(0xfeedfeed));
                }
                assertThat(bbread.readableBytes(), equalTo(0));
            } finally {
                bbread.release();
            }

            bbread = reader.readEntryAt(offset2);
            try {
                assertThat(bbread.readableBytes(), equalTo(entrySize));
                while (bbread.readableBytes() >= Integer.BYTES) {
                    assertThat(bbread.readInt(), equalTo(0xfedefede));
                }
                assertThat(bbread.readableBytes(), equalTo(0));
            } finally {
                bbread.release();
            }
        }
    }

    private static void writeFileWithPattern(File directory, int logId,
                                             int pattern, int blockIncrement, int fileSize) throws Exception {
        try (BufferPool buffers = new BufferPool(new NativeIOImpl(), ByteBufAllocator.DEFAULT, Buffer.ALIGNMENT, 8);
             LogWriter writer = new DirectWriter(logId, logFilename(directory, logId),
                                                 fileSize, MoreExecutors.newDirectExecutorService(),
                                                 buffers, new NativeIOImpl(), Slogger.CONSOLE)) {

            for (int written = 0; written < fileSize; written += Buffer.ALIGNMENT) {
                ByteBuf bb = Unpooled.buffer(Buffer.ALIGNMENT);
                TestBuffer.fillByteBuf(bb, pattern);
                writer.writeAt(written, bb);
                bb.release();
                pattern += blockIncrement;
            }
            writer.flush();
        }
    }

}
