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

    @Test
    public void calculateCheckSum() {
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
        b2.writeBytes(hugeData);

        // Calculate: case-1.
        int checksumRes1 = Crc32cIntChecksum.computeChecksum(bTotal);
        log.info("checksumRes1: {}", checksumRes1);

        // Calculate: case-2.
        int b1CheckSum = Crc32cIntChecksum.computeChecksum(b1);
        log.info("b1CheckSum: {}", b1CheckSum);
        int checksumRes2 = Crc32cIntChecksum.resumeChecksum(b1CheckSum,
                new CompositeByteBuf(ByteBufAllocator.DEFAULT, false, 2,  b2, b3));
        log.info("checksumRes2: {}", checksumRes2);

        // Verify: the results of both ways to calculate the checksum are same.
        Assert.assertEquals(checksumRes1, checksumRes2);

        // cleanup.
        bTotal.release();
        b1.release();
        b2.release();
        b3.release();
    }

    @Test
    public void calculateCheckSum2() {
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
        b2.writeBytes(hugeData);

        // Calculate: case-1.
        int checksumRes1 = Crc32cIntChecksum.computeChecksum(bTotal);
        log.info("checksumRes1: {}", checksumRes1);

        // Calculate: case-2.
        int b1CheckSum = Crc32cIntChecksum.computeChecksum(b1);
        log.info("b1CheckSum: {}", b1CheckSum);
        int checksumRes2 = Crc32cIntChecksum.resumeChecksum(b1CheckSum,
                new NoArrayNoMemoryAddrByteBuff(new CompositeByteBuf(ByteBufAllocator.DEFAULT, false, 2,  b2, b3)));
        log.info("checksumRes2: {}", checksumRes2);

        // Verify: the results of both ways to calculate the checksum are same.
        Assert.assertEquals(checksumRes1, checksumRes2);

        // cleanup.
        bTotal.release();
        b1.release();
        b2.release();
        b3.release();
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