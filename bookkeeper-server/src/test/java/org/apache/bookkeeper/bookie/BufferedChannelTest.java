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
    public void testBufferedChannelFlushForceWrite() throws Exception {
        testBufferedChannel(5000, 30, 0, true, true);
    }

    public void testBufferedChannel(int byteBufLength, int numOfWrites, int unpersistedBytesBound, boolean flush,
            boolean shouldForceWrite) throws Exception {
        File newLogFile = File.createTempFile("test", "log");
        newLogFile.deleteOnExit();
        FileChannel fileChannel = new RandomAccessFile(newLogFile, "rw").getChannel();

        BufferedChannel logChannel = new BufferedChannel(fileChannel, 65536, 512, unpersistedBytesBound);

        ByteBuf dataBuf = generateEntry(byteBufLength);
        dataBuf.markReaderIndex();
        dataBuf.markWriterIndex();

        for (int i = 0; i < numOfWrites; i++) {
            logChannel.write(dataBuf);
            dataBuf.resetReaderIndex();
            dataBuf.resetWriterIndex();
        }

        if (flush) {
            logChannel.flush(shouldForceWrite);
        }

        int expectedNumOfUnpersistedBytes = 0;

        if (flush && shouldForceWrite) {
            /*
             * if flush call is made with shouldForceWrite,
             * then expectedNumOfUnpersistedBytes should be zero.
             */
            expectedNumOfUnpersistedBytes = 0;
        } else {
            expectedNumOfUnpersistedBytes = (byteBufLength * numOfWrites) - unpersistedBytesBound;
        }

        Assert.assertEquals("Unpersisted bytes", expectedNumOfUnpersistedBytes, logChannel.getUnpersistedBytes());
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
