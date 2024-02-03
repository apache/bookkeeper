/*
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
 */
package com.scurrilous.circe.checksum;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.DuplicatedByteBuf;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class Java9IntHashTest {

    private ByteBuf[] generateByteBuffers() {
        Random random = new Random();
        int hugeDataLen = 4096 * 3;
        byte[] hugeData = new byte[hugeDataLen];
        for (int i = 0; i < hugeDataLen; i ++) {
            hugeData[i] = (byte) (random.nextInt() % 127);
        }

        // b_total = b1 + b2 + b3;
        ByteBuf bTotal = ByteBufAllocator.DEFAULT.heapBuffer(6 + hugeDataLen);
        bTotal.writeBytes(new byte[]{1,2,3,4,5,6});
        bTotal.writeBytes(hugeData);
        ByteBuf b1 = ByteBufAllocator.DEFAULT.heapBuffer(3);
        b1.writeBytes(new byte[]{1,2,3});
        ByteBuf b2 = ByteBufAllocator.DEFAULT.heapBuffer(3);
        b2.writeBytes(new byte[]{4,5,6});
        ByteBuf b3 = ByteBufAllocator.DEFAULT.heapBuffer(hugeDataLen);
        b3.writeBytes(hugeData);

        return new ByteBuf[]{bTotal, b1, new CompositeByteBuf(ByteBufAllocator.DEFAULT, false, 2,  b2, b3)};
    }

    @Test
    public void calculateCheckSumUsingCompositeByteBuf() {
        // byteBuffers[0] = byteBuffers[1] + byteBuffers[2].
        // byteBuffers[2] is a composite ByteBuf.
        ByteBuf[] byteBuffers = generateByteBuffers();
        ByteBuf bTotal = byteBuffers[0];
        ByteBuf b1 = byteBuffers[1];
        ByteBuf b2 = byteBuffers[2];

        // Calculate: case-1.
        int checksumRes1 = Crc32cIntChecksum.computeChecksum(bTotal);

        // Calculate: case-2.
        int b1CheckSum = Crc32cIntChecksum.computeChecksum(b1);
        int checksumRes2 = Crc32cIntChecksum.resumeChecksum(b1CheckSum, b2);

        // Verify: the results of both ways to calculate the checksum are same.
        Assert.assertEquals(checksumRes1, checksumRes2);

        // cleanup.
        bTotal.release();
        b1.release();
        b2.release();
    }

    @Test
    public void calculateCheckSumUsingNoArrayNoMemoryAddrByteBuf() {
        // byteBuffers[0] = byteBuffers[1] + byteBuffers[2].
        // byteBuffers[2] is a composite ByteBuf.
        ByteBuf[] byteBuffers = generateByteBuffers();
        ByteBuf bTotal = byteBuffers[0];
        ByteBuf b1 = byteBuffers[1];
        ByteBuf b2 = new NoArrayNoMemoryAddrByteBuff(byteBuffers[2]);

        // Calculate: case-1.
        int checksumRes1 = Crc32cIntChecksum.computeChecksum(bTotal);

        // Calculate: case-2.
        int b1CheckSum = Crc32cIntChecksum.computeChecksum(b1);
        int checksumRes2 = Crc32cIntChecksum.resumeChecksum(b1CheckSum, b2);

        // Verify: the results of both ways to calculate the checksum are same.
        Assert.assertEquals(checksumRes1, checksumRes2);

        // cleanup.
        bTotal.release();
        b1.release();
        b2.release();
    }

    public static class NoArrayNoMemoryAddrByteBuff extends DuplicatedByteBuf {

        public NoArrayNoMemoryAddrByteBuff(ByteBuf buffer) {
            super(buffer);
        }

        @Override
        public boolean hasArray(){
            return false;
        }

        @Override
        public boolean hasMemoryAddress(){
            return false;
        }
    }
}