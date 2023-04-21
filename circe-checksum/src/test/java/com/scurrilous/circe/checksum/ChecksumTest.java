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

import static com.scurrilous.circe.params.CrcParameters.CRC32C;
import static org.junit.Assert.assertEquals;

import com.scurrilous.circe.IncrementalIntHash;
import com.scurrilous.circe.IncrementalLongHash;
import com.scurrilous.circe.crc.StandardCrcProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

/**
 * Verify circe checksum.
 */
public class ChecksumTest {

    @Test
    public void testCrc32cValue() {
        final byte[] bytes = "Some String".getBytes();
        int checksum = Crc32cIntChecksum.computeChecksum(Unpooled.wrappedBuffer(bytes));

        assertEquals(608512271, checksum);
    }

    @Test
    public void testCrc32cValueResume() {
        final byte[] bytes = "Some String".getBytes();
        int checksum = Crc32cIntChecksum.resumeChecksum(0, Unpooled.wrappedBuffer(bytes), 0, bytes.length);

        assertEquals(608512271, checksum);
    }

    @Test
    public void testCrc32cValueIncremental() {
        final byte[] bytes = "Some String".getBytes();

        int checksum = Crc32cIntChecksum.computeChecksum(Unpooled.wrappedBuffer(bytes));
        assertEquals(608512271, checksum);

        checksum = Crc32cIntChecksum.computeChecksum(Unpooled.wrappedBuffer(bytes, 0, 1));
        for (int i = 1; i < bytes.length; i++) {
            checksum = Crc32cIntChecksum.resumeChecksum(checksum, Unpooled.wrappedBuffer(bytes), i, 1);
        }
        assertEquals(608512271, checksum);

        checksum = Crc32cIntChecksum.computeChecksum(Unpooled.wrappedBuffer(bytes, 0, 4));
        checksum = Crc32cIntChecksum.resumeChecksum(checksum, Unpooled.wrappedBuffer(bytes), 4, 7);
        assertEquals(608512271, checksum);


        ByteBuf buffer = Unpooled.wrappedBuffer(bytes, 0, 4);
        checksum = Crc32cIntChecksum.computeChecksum(buffer);
        checksum = Crc32cIntChecksum.resumeChecksum(
            checksum, Unpooled.wrappedBuffer(bytes), 4, bytes.length - 4);

        assertEquals(608512271, checksum);
    }

    @Test
    public void testCrc32cLongValue() {
        final byte[] bytes = "Some String".getBytes();
        long checksum = Crc32cIntChecksum.computeChecksum(Unpooled.wrappedBuffer(bytes));

        assertEquals(608512271L, checksum);
    }

    @Test
    public void testCrc32cLongValueResume() {
        final byte[] bytes = "Some String".getBytes();
        long checksum = Crc32cIntChecksum.resumeChecksum(0, Unpooled.wrappedBuffer(bytes), 0, bytes.length);

        assertEquals(608512271L, checksum);
    }

    @Test
    public void testCRC32CIncrementalLong() {
        IncrementalLongHash crc32c = new StandardCrcProvider().getIncrementalLong(CRC32C);
        String data = "data";
        String combine = data + data;

        long combineChecksum = crc32c.calculate(combine.getBytes());
        long dataChecksum = crc32c.calculate(data.getBytes());
        long incrementalChecksum = crc32c.resume(dataChecksum, data.getBytes());
        assertEquals(combineChecksum, incrementalChecksum);
    }

    @Test
    public void testCRC32CIncrementalInt() {
        IncrementalIntHash crc32c = new StandardCrcProvider().getIncrementalInt(CRC32C);
        String data = "data";
        String combine = data + data;

        int combineChecksum = crc32c.calculate(combine.getBytes());
        int dataChecksum = crc32c.calculate(data.getBytes());
        int incrementalChecksum = crc32c.resume(dataChecksum, data.getBytes());
        assertEquals(combineChecksum, incrementalChecksum);
    }

}
