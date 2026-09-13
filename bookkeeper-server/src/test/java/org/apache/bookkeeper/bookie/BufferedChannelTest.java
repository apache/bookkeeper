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

import static org.junit.Assert.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for BufferedChannel.
 */
public class BufferedChannelTest {

    private static Random rand = new Random();
    private static final int INTERNAL_BUFFER_WRITE_CAPACITY = 65536;
    private static final int INTERNAL_BUFFER_READ_CAPACITY = 512;

    @Test
    public void testBufferedChannelWithNoBoundOnUnpersistedBytes() throws Exception {
        testBufferedChannel(5000, 30, 0, false, false);
    }

    @Test
    public void testBufferedChannelWithBoundOnUnpersistedBytes() throws Exception {
        testBufferedChannel(5000, 30, 5000 * 28, false, false);
    }

    @Test
    public void testBufferedChannelWithBoundOnUnpersistedBytesAndFlush() throws Exception {
        testBufferedChannel(5000, 30, 5000 * 28, true, false);
    }

    @Test
    public void testBufferedChannelFlushNoForceWrite() throws Exception {
        testBufferedChannel(5000, 30, 0, true, false);
    }

    @Test
    public void testBufferedChannelForceWriteNoFlush() throws Exception {
        testBufferedChannel(5000, 30, 0, false, true);
    }

    @Test
    public void testBufferedChannelFlushForceWrite() throws Exception {
        testBufferedChannel(5000, 30, 0, true, true);
    }

    @Test
    public void testPartialFlushFailurePoisonsBufferedChannel() throws Exception {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();
        FileChannel delegate = new RandomAccessFile(newLogFile, "rw").getChannel();
        PartialFailingFileChannel fileChannel = new PartialFailingFileChannel(delegate, 8);

        BufferedChannel logChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel,
                16, INTERNAL_BUFFER_READ_CAPACITY, 0);

        try {
            logChannel.write(Unpooled.wrappedBuffer(new byte[16]));
            Assert.fail("Expected the internal flush to fail");
        } catch (IOException expected) {
            // Expected.
        }

        Assert.assertEquals(0, logChannel.position());
        Assert.assertEquals(8, fileChannel.position());

        try {
            logChannel.write(Unpooled.wrappedBuffer(new byte[1]));
            Assert.fail("Expected writes after a partial flush failure to fail");
        } catch (IOException expected) {
            // Expected.
        }
        try {
            logChannel.flush();
            Assert.fail("Expected flush after a partial flush failure to fail");
        } catch (IOException expected) {
            // Expected.
        }
        try {
            logChannel.forceWrite(false);
            Assert.fail("Expected forceWrite after a partial flush failure to fail");
        } catch (IOException expected) {
            // Expected.
        }

        Assert.assertEquals(0, logChannel.position());
        Assert.assertEquals(8, fileChannel.position());

        logChannel.close();
    }

    @Test
    public void testPartialFlushFailurePoisonsBufferedChannelForTwoBufferWrite() throws Exception {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();
        FileChannel delegate = new RandomAccessFile(newLogFile, "rw").getChannel();
        PartialFailingFileChannel fileChannel = new PartialFailingFileChannel(delegate, 8);

        BufferedChannel logChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel,
                16, INTERNAL_BUFFER_READ_CAPACITY, 0);

        try {
            logChannel.write(Unpooled.wrappedBuffer(new byte[8]), Unpooled.wrappedBuffer(new byte[8]));
            Assert.fail("Expected the internal flush to fail");
        } catch (IOException expected) {
            // Expected.
        }

        Assert.assertEquals(0, logChannel.position());
        Assert.assertEquals(8, fileChannel.position());

        try {
            logChannel.write(Unpooled.wrappedBuffer(new byte[1]), Unpooled.wrappedBuffer(new byte[1]));
            Assert.fail("Expected writes after a partial flush failure to fail");
        } catch (IOException expected) {
            // Expected.
        }
        try {
            logChannel.flush();
            Assert.fail("Expected flush after a partial flush failure to fail");
        } catch (IOException expected) {
            // Expected.
        }

        Assert.assertEquals(0, logChannel.position());
        Assert.assertEquals(8, fileChannel.position());

        logChannel.close();
    }

    @Test
    public void testPartialFlushFailurePreventsLedgersMapHeaderUpdate() throws Exception {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();
        FileChannel delegate = new RandomAccessFile(newLogFile, "rw").getChannel();
        delegate.position(DefaultEntryLogger.LOGFILE_HEADER_SIZE);
        PartialFailingFileChannel fileChannel = new PartialFailingFileChannel(delegate, 8);

        DefaultEntryLogger.BufferedLogChannel logChannel = new DefaultEntryLogger.BufferedLogChannel(
                UnpooledByteBufAllocator.DEFAULT, fileChannel, 16, INTERNAL_BUFFER_READ_CAPACITY, 1L, newLogFile, 0);

        try {
            logChannel.write(Unpooled.wrappedBuffer(new byte[16]));
            Assert.fail("Expected the internal flush to fail");
        } catch (IOException expected) {
            // Expected.
        }

        logChannel.registerWrittenEntry(1234L, 99L);
        try {
            logChannel.appendLedgersMap();
            Assert.fail("Expected appendLedgersMap after a partial flush failure to fail");
        } catch (IOException expected) {
            // Expected.
        }

        ByteBuffer mapInfo = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        Assert.assertEquals(mapInfo.capacity(),
                fileChannel.read(mapInfo, DefaultEntryLogger.LEDGERS_MAP_OFFSET_POSITION));
        mapInfo.flip();
        Assert.assertEquals(0L, mapInfo.getLong());
        Assert.assertEquals(0, mapInfo.getInt());

        logChannel.close();
    }

    @Test
    public void testForceWriteFailurePoisonsBufferedChannel() throws Exception {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();
        FileChannel delegate = new RandomAccessFile(newLogFile, "rw").getChannel();
        PartialFailingFileChannel fileChannel = PartialFailingFileChannel.failingForce(delegate);

        BufferedChannel logChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel,
                16, INTERNAL_BUFFER_READ_CAPACITY, 0);

        logChannel.write(Unpooled.wrappedBuffer(new byte[] { 1 }));
        logChannel.flush();

        try {
            logChannel.forceWrite(false);
            Assert.fail("Expected forceWrite failure");
        } catch (IOException expected) {
            // Expected.
        }
        try {
            logChannel.write(Unpooled.wrappedBuffer(new byte[] { 2 }));
            Assert.fail("Expected writes after forceWrite failure to fail");
        } catch (IOException expected) {
            // Expected.
        }

        Assert.assertEquals(1, logChannel.position());

        logChannel.close();
    }

    @Test
    public void testLedgersMapHeaderPositionedWriteIsFullyWritten() throws Exception {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();
        FileChannel delegate = new RandomAccessFile(newLogFile, "rw").getChannel();
        writeEntryLogHeader(delegate);
        PartialFailingFileChannel fileChannel = PartialFailingFileChannel.shortPositionedWrite(
                delegate, DefaultEntryLogger.LEDGERS_MAP_OFFSET_POSITION, Long.BYTES);

        DefaultEntryLogger.BufferedLogChannel logChannel = new DefaultEntryLogger.BufferedLogChannel(
                UnpooledByteBufAllocator.DEFAULT, fileChannel, 64, INTERNAL_BUFFER_READ_CAPACITY, 1L, newLogFile, 0);

        logChannel.write(Unpooled.wrappedBuffer(new byte[] { 1 }));
        logChannel.flush();

        long actualMapOffset = logChannel.position();
        logChannel.registerWrittenEntry(1234L, 1L);
        logChannel.appendLedgersMap();

        ByteBuffer mapInfo = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        Assert.assertEquals(mapInfo.capacity(),
                fileChannel.read(mapInfo, DefaultEntryLogger.LEDGERS_MAP_OFFSET_POSITION));
        mapInfo.flip();
        Assert.assertEquals(actualMapOffset, mapInfo.getLong());
        Assert.assertEquals(1, mapInfo.getInt());
        Assert.assertTrue(fileChannel.shortPositionedWriteDone);

        logChannel.close();
    }

    @Test
    public void testLedgersMapHeaderWriteFailurePoisonsBufferedChannel() throws Exception {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();
        FileChannel delegate = new RandomAccessFile(newLogFile, "rw").getChannel();
        writeEntryLogHeader(delegate);
        PartialFailingFileChannel fileChannel = PartialFailingFileChannel.failingPositionedWrite(
                delegate, DefaultEntryLogger.LEDGERS_MAP_OFFSET_POSITION);

        DefaultEntryLogger.BufferedLogChannel logChannel = new DefaultEntryLogger.BufferedLogChannel(
                UnpooledByteBufAllocator.DEFAULT, fileChannel, 64, INTERNAL_BUFFER_READ_CAPACITY, 1L, newLogFile, 0);

        logChannel.write(Unpooled.wrappedBuffer(new byte[] { 1 }));
        logChannel.flush();
        long mapOffset = logChannel.position();
        logChannel.registerWrittenEntry(1234L, 1L);

        try {
            logChannel.appendLedgersMap();
            Assert.fail("Expected header write failure");
        } catch (IOException expected) {
            // Expected.
        }
        try {
            logChannel.write(Unpooled.wrappedBuffer(new byte[] { 2 }));
            Assert.fail("Expected writes after header write failure to fail");
        } catch (IOException expected) {
            // Expected.
        }

        ByteBuffer mapInfo = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        Assert.assertEquals(mapInfo.capacity(),
                fileChannel.read(mapInfo, DefaultEntryLogger.LEDGERS_MAP_OFFSET_POSITION));
        mapInfo.flip();
        Assert.assertEquals(0L, mapInfo.getLong());
        Assert.assertEquals(0, mapInfo.getInt());

        Assert.assertEquals(mapOffset + DefaultEntryLogger.LEDGERS_MAP_HEADER_SIZE
                + DefaultEntryLogger.LEDGERS_MAP_ENTRY_SIZE, logChannel.position());

        logChannel.close();
    }

    public void testBufferedChannel(int byteBufLength, int numOfWrites, int unpersistedBytesBound, boolean flush,
            boolean shouldForceWrite) throws Exception {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();
        FileChannel fileChannel = new RandomAccessFile(newLogFile, "rw").getChannel();

        BufferedChannel logChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel,
                INTERNAL_BUFFER_WRITE_CAPACITY, INTERNAL_BUFFER_READ_CAPACITY, unpersistedBytesBound);

        ByteBuf dataBuf = generateEntry(byteBufLength);
        dataBuf.markReaderIndex();
        dataBuf.markWriterIndex();

        for (int i = 0; i < numOfWrites; i++) {
            logChannel.write(dataBuf);
            dataBuf.resetReaderIndex();
            dataBuf.resetWriterIndex();
        }

        if (flush && shouldForceWrite) {
            logChannel.flushAndForceWrite(false);
        } else if (flush) {
            logChannel.flush();
        } else if (shouldForceWrite) {
            logChannel.forceWrite(false);
        }

        int expectedNumOfUnpersistedBytes = 0;

        if (flush && shouldForceWrite) {
            /*
             * if flush call is made with shouldForceWrite,
             * then expectedNumOfUnpersistedBytes should be zero.
             */
            expectedNumOfUnpersistedBytes = 0;
        } else if (!flush && shouldForceWrite) {
            /*
             * if flush is not called then internal write buffer is not flushed,
             * but while adding entries to BufferedChannel if writeBuffer has
             * reached its capacity then it will call flush method, and the data
             * gets added to the file buffer. So though explicitly we are not
             * calling flush method, implicitly flush gets called when
             * writeBuffer reaches its capacity.
             */
            expectedNumOfUnpersistedBytes = (byteBufLength * numOfWrites) % INTERNAL_BUFFER_WRITE_CAPACITY;
        } else {
            expectedNumOfUnpersistedBytes = (byteBufLength * numOfWrites) - unpersistedBytesBound;
        }

        if (unpersistedBytesBound > 0) {
            Assert.assertEquals("Unpersisted bytes", expectedNumOfUnpersistedBytes, logChannel.getUnpersistedBytes());
        }
        logChannel.close();
        fileChannel.close();
    }

    @Test
    public void testWriteTwoBuffers() throws Exception {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();
        FileChannel fileChannel = new RandomAccessFile(newLogFile, "rw").getChannel();

        // small write capacity so the pairs below cross the buffer boundary at varied offsets,
        // including a payload equal to the capacity and one larger than it
        int writeCapacity = 64;
        BufferedChannel logChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fileChannel,
                writeCapacity, INTERNAL_BUFFER_READ_CAPACITY, 0);

        int[] payloadSizes = { 25, 31, 60, 3, 64, 128, 1, 41 };
        ByteBuf expected = Unpooled.buffer();
        ByteBuf lenBuf = Unpooled.buffer(4);

        for (int payloadSize : payloadSizes) {
            ByteBuf payload = generateEntry(payloadSize);
            lenBuf.clear();
            lenBuf.writeInt(payloadSize);

            expected.writeBytes(lenBuf.slice());
            expected.writeBytes(payload.slice());

            logChannel.write(lenBuf, payload);
        }

        Assert.assertEquals(expected.readableBytes(), logChannel.position());

        logChannel.flush();

        byte[] fileBytes = Files.readAllBytes(newLogFile.toPath());
        Assert.assertArrayEquals(ByteBufUtil.getBytes(expected), fileBytes);

        logChannel.close();
        fileChannel.close();
    }

    @Test
    public void testBufferedChannelReadWhenDestBufSizeExceedsReadLength() throws IOException {
        doTestBufferedChannelReadThrowing(100, 60);
    }

    @Test
    public void testBufferedChannelReadWhenDestBufSizeDoesNotExceedReadLength() throws IOException {
        doTestBufferedChannelReadThrowing(100, 110);
    }

    private void doTestBufferedChannelReadThrowing(int destBufSize, int readLength) throws IOException {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();

        try (RandomAccessFile raf = new RandomAccessFile(newLogFile, "rw")) {
            FileChannel fileChannel = raf.getChannel();

            try (BufferedChannel bufferedChannel = new BufferedChannel(
                UnpooledByteBufAllocator.DEFAULT, fileChannel,
                INTERNAL_BUFFER_WRITE_CAPACITY, INTERNAL_BUFFER_READ_CAPACITY, 0)) {

                bufferedChannel.write(generateEntry(500));

                ByteBuf destBuf = UnpooledByteBufAllocator.DEFAULT.buffer(destBufSize);

                if (destBufSize < readLength) {
                    assertThrows(IllegalArgumentException.class,
                        () -> bufferedChannel.read(destBuf, 0, readLength));
                } else {
                    bufferedChannel.read(destBuf, 0, readLength);
                }
            }
        }
    }

    private static ByteBuf generateEntry(int length) {
        byte[] data = new byte[length];
        ByteBuf bb = Unpooled.buffer(length);
        rand.nextBytes(data);
        bb.writeBytes(data);
        return bb;
    }

    private static void writeEntryLogHeader(FileChannel fileChannel) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(DefaultEntryLogger.LOGFILE_HEADER_SIZE);
        header.put("BKLO".getBytes("UTF-8"));
        header.putInt(DefaultEntryLogger.HEADER_CURRENT_VERSION);
        header.position(DefaultEntryLogger.LOGFILE_HEADER_SIZE);
        header.flip();
        while (header.hasRemaining()) {
            fileChannel.write(header);
        }
        fileChannel.position(DefaultEntryLogger.LOGFILE_HEADER_SIZE);
    }

    private static final class PartialFailingFileChannel extends FileChannel {
        private final FileChannel delegate;
        private final int bytesBeforeFailure;
        private final boolean failRegularWrite;
        private final long shortPositionedWritePosition;
        private final int shortPositionedWriteBytes;
        private final long failingPositionedWritePosition;
        private final boolean failForce;
        private boolean partialWriteDone;
        private boolean failureInjected;
        private boolean shortPositionedWriteDone;
        private boolean positionedWriteFailureInjected;

        private PartialFailingFileChannel(FileChannel delegate, int bytesBeforeFailure) {
            this(delegate, bytesBeforeFailure, true, -1L, -1, -1L, false);
        }

        private PartialFailingFileChannel(FileChannel delegate, int bytesBeforeFailure,
                                          boolean failRegularWrite, long shortPositionedWritePosition,
                                          int shortPositionedWriteBytes, long failingPositionedWritePosition,
                                          boolean failForce) {
            this.delegate = delegate;
            this.bytesBeforeFailure = bytesBeforeFailure;
            this.failRegularWrite = failRegularWrite;
            this.shortPositionedWritePosition = shortPositionedWritePosition;
            this.shortPositionedWriteBytes = shortPositionedWriteBytes;
            this.failingPositionedWritePosition = failingPositionedWritePosition;
            this.failForce = failForce;
        }

        private static PartialFailingFileChannel shortPositionedWrite(FileChannel delegate, long position, int bytes) {
            return new PartialFailingFileChannel(delegate, 0, false, position, bytes, -1L, false);
        }

        private static PartialFailingFileChannel failingPositionedWrite(FileChannel delegate, long position) {
            return new PartialFailingFileChannel(delegate, 0, false, -1L, -1, position, false);
        }

        private static PartialFailingFileChannel failingForce(FileChannel delegate) {
            return new PartialFailingFileChannel(delegate, 0, false, -1L, -1, -1L, true);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            if (failRegularWrite && !partialWriteDone) {
                int oldLimit = src.limit();
                src.limit(src.position() + bytesBeforeFailure);
                int written = delegate.write(src);
                src.limit(oldLimit);
                partialWriteDone = true;
                return written;
            } else if (failRegularWrite && !failureInjected) {
                failureInjected = true;
                throw new IOException("simulated write failure after partial write");
            }
            return delegate.write(src);
        }

        @Override
        public long position() throws IOException {
            return delegate.position();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            delegate.position(newPosition);
            return this;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return delegate.read(dst);
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return delegate.read(dsts, offset, length);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            return delegate.write(srcs, offset, length);
        }

        @Override
        public long size() throws IOException {
            return delegate.size();
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            delegate.truncate(size);
            return this;
        }

        @Override
        public void force(boolean metaData) throws IOException {
            if (failForce) {
                throw new IOException("simulated force failure");
            }
            delegate.force(metaData);
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
            return delegate.transferTo(position, count, target);
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
            return delegate.transferFrom(src, position, count);
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            return delegate.read(dst, position);
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            if (!positionedWriteFailureInjected && position == failingPositionedWritePosition) {
                positionedWriteFailureInjected = true;
                throw new IOException("simulated positioned write failure");
            }
            if (!shortPositionedWriteDone && position == shortPositionedWritePosition) {
                int oldLimit = src.limit();
                src.limit(src.position() + Math.min(shortPositionedWriteBytes, src.remaining()));
                int written = delegate.write(src, position);
                src.limit(oldLimit);
                shortPositionedWriteDone = true;
                return written;
            }
            return delegate.write(src, position);
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            return delegate.map(mode, position, size);
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            return delegate.lock(position, size, shared);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return delegate.tryLock(position, size, shared);
        }

        @Override
        protected void implCloseChannel() throws IOException {
            delegate.close();
        }
    }
}
