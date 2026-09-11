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
import org.junit.Assume;
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

    /**
     * The three ways {@link Java9IntHash#resume(int, ByteBuf, int, int)} can reach the data, checked
     * against {@link Java8IntHash} as an independent implementation of the same algorithm.
     *
     * <p>The direct path is the one worth pinning: it is the only caller of the JDK's
     * {@code updateDirectByteBuffer}, whose {@code (int, long, int, int)int} shape has to be matched
     * exactly by the method handle invocation. Getting that wrong is not a compile error — it fails
     * at runtime with a {@code WrongMethodTypeException} — and none of the other cases would catch
     * it, since they take the {@code updateBytes} path instead.
     */
    @Test
    public void matchesJava8ImplementationOnEveryBufferKind() {
        Assume.assumeTrue("java.util.zip.CRC32C is not reachable, so Java9IntHash is not in use",
                Java9IntHash.HAS_JAVA9_CRC32C);

        byte[] data = new byte[8192];
        new Random(42).nextBytes(data);

        ByteBuf direct = ByteBufAllocator.DEFAULT.directBuffer(data.length);
        ByteBuf heap = ByteBufAllocator.DEFAULT.heapBuffer(data.length);
        try {
            direct.writeBytes(data);
            heap.writeBytes(data);
            Assert.assertTrue("expected a buffer with a memory address to cover the direct path",
                    direct.hasMemoryAddress());

            Java9IntHash java9 = new Java9IntHash();
            Java8IntHash java8 = new Java8IntHash();

            Assert.assertEquals(java8.calculate(direct), java9.calculate(direct));
            Assert.assertEquals(java8.calculate(heap), java9.calculate(heap));
            Assert.assertEquals(java8.calculate(new NoArrayNoMemoryAddrByteBuff(heap)),
                    java9.calculate(new NoArrayNoMemoryAddrByteBuff(heap)));

            // Resuming has to agree too: it is the incremental form the ledger write path uses.
            int half = data.length / 2;
            int java9Resumed = java9.resume(java9.calculate(direct.slice(0, half)),
                    direct.slice(half, data.length - half));
            Assert.assertEquals(java8.calculate(direct), java9Resumed);
        } finally {
            direct.release();
            heap.release();
        }
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