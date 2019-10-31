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
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
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
}
