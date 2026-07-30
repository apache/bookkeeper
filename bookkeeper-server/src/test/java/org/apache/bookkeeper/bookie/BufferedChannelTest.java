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
    public void testPositionCanLagFileChannelAfterPartialFlushFailure() throws Exception {
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

        logChannel.write(Unpooled.wrappedBuffer(new byte[1]));

        Assert.assertEquals(1, logChannel.position());
        Assert.assertEquals(24, fileChannel.position());
        Assert.assertEquals(23, fileChannel.position() - logChannel.position());

        logChannel.close();
    }

    @Test
    public void testLedgersMapHeaderUsesStalePositionAfterPartialFlushFailure() throws Exception {
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

        logChannel.write(Unpooled.wrappedBuffer(new byte[] { 1 }));
        logChannel.flush();

        long staleMapOffset = logChannel.position();
        long actualMapOffset = fileChannel.position();
        Assert.assertTrue(actualMapOffset > staleMapOffset);

        logChannel.registerWrittenEntry(1234L, 99L);
        logChannel.appendLedgersMap();

        ByteBuffer mapInfo = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        Assert.assertEquals(mapInfo.capacity(),
                fileChannel.read(mapInfo, DefaultEntryLogger.LEDGERS_MAP_OFFSET_POSITION));
        mapInfo.flip();
        Assert.assertEquals(staleMapOffset, mapInfo.getLong());
        Assert.assertEquals(1, mapInfo.getInt());

        ByteBuffer actualMap = ByteBuffer.allocate(DefaultEntryLogger.LEDGERS_MAP_HEADER_SIZE);
        Assert.assertEquals(actualMap.capacity(), fileChannel.read(actualMap, actualMapOffset));
        actualMap.flip();
        Assert.assertEquals(DefaultEntryLogger.LEDGERS_MAP_HEADER_SIZE + DefaultEntryLogger.LEDGERS_MAP_ENTRY_SIZE
                - Integer.BYTES, actualMap.getInt());
        Assert.assertEquals(DefaultEntryLogger.INVALID_LID, actualMap.getLong());
        Assert.assertEquals(DefaultEntryLogger.LEDGERS_MAP_ENTRY_ID, actualMap.getLong());
        Assert.assertEquals(1, actualMap.getInt());

        ByteBuffer staleBytes = ByteBuffer.allocate(DefaultEntryLogger.LEDGERS_MAP_HEADER_SIZE);
        Assert.assertEquals(staleBytes.capacity(), fileChannel.read(staleBytes, staleMapOffset));
        staleBytes.flip();
        Assert.assertNotEquals(DefaultEntryLogger.INVALID_LID, staleBytes.getLong(Integer.BYTES));

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

    private static ByteBuf generateEntry(int length) {
        byte[] data = new byte[length];
        ByteBuf bb = Unpooled.buffer(length);
        rand.nextBytes(data);
        bb.writeBytes(data);
        return bb;
    }

    private static final class PartialFailingFileChannel extends FileChannel {
        private final FileChannel delegate;
        private final int bytesBeforeFailure;
        private boolean partialWriteDone;
        private boolean failureInjected;

        private PartialFailingFileChannel(FileChannel delegate, int bytesBeforeFailure) {
            this.delegate = delegate;
            this.bytesBeforeFailure = bytesBeforeFailure;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            if (!partialWriteDone) {
                int oldLimit = src.limit();
                src.limit(src.position() + bytesBeforeFailure);
                int written = delegate.write(src);
                src.limit(oldLimit);
                partialWriteDone = true;
                return written;
            } else if (!failureInjected) {
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
